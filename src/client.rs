//! Client side: outbound sessions to every configured peer.
//!
//! Keeps one primary heartbeat session open per peer, plus a session pool
//! per peer when `[client.pool]` is set (see `pool.rs`), and reconciles the
//! set of peers when the config is reloaded.

use crate::config::{ClientConfig, Config, PeerConfig};
use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::{now_f64, protocol};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// A running outbound session task for one peer.
#[derive(Debug)]
struct PeerTask {
    /// Peer config the task was started with; a change restarts the task.
    peer: PeerConfig,
    /// Stops this task only.
    cancel: CancellationToken,
    /// Completes when the task has stopped.
    handle: JoinHandle<()>,
}

impl PeerTask {
    /// Cancel the task and wait for it to finish, logging if it panicked.
    async fn stop(self) {
        self.cancel.cancel();
        if let Err(e) = self.handle.await {
            error!(
                "Peer session task for {} ended abnormally: {}",
                self.peer.name, e
            );
        }
    }
}

/// Maintain outbound sessions to every configured peer until `cancel` fires
/// or the config sender is dropped. Peers added, removed, or changed in a
/// reloaded config are started, stopped, or restarted.
pub async fn run_manager(
    metrics: Arc<Metrics>,
    mut config_rx: watch::Receiver<Config>,
    cancel: CancellationToken,
) {
    let mut tasks: HashMap<String, PeerTask> = HashMap::new();
    let mut last_client_cfg: Option<ClientConfig> = None;

    loop {
        let config = config_rx.borrow_and_update().clone();
        let node_name = config.node.name.clone();
        let Some(client_cfg) = config.client.clone() else {
            // No client config; cancel any existing tasks and wait for next change.
            let drained: Vec<_> = tasks.drain().collect();
            for (name, task) in drained {
                info!("Removing peer session (no [client] config): {}", name);
                task.stop().await;
            }
            last_client_cfg = None;
            tokio::select! {
                result = config_rx.changed() => { if result.is_err() { break; } }
                () = cancel.cancelled() => break,
            }
            continue;
        };

        // Warn if [client] settings changed; they apply to new sessions only (restart required).
        if let Some(ref old_cfg) = last_client_cfg {
            if old_cfg != &client_cfg {
                if old_cfg.heartbeat_interval != client_cfg.heartbeat_interval {
                    warn!(
                        "client.heartbeat_interval changed ({} → {}); restart required for running sessions",
                        old_cfg.heartbeat_interval, client_cfg.heartbeat_interval
                    );
                }
                if old_cfg.max_misses != client_cfg.max_misses {
                    warn!(
                        "client.max_misses changed ({} → {}); restart required for running sessions",
                        old_cfg.max_misses, client_cfg.max_misses
                    );
                }
                if old_cfg.reconnect_delay != client_cfg.reconnect_delay {
                    warn!(
                        "client.reconnect_delay changed ({} → {}); restart required for running sessions",
                        old_cfg.reconnect_delay, client_cfg.reconnect_delay
                    );
                }
                if old_cfg.pool != client_cfg.pool {
                    warn!("client.pool changed; restart required for running pools");
                }
            }
        }
        last_client_cfg = Some(client_cfg.clone());

        let new_peers: HashMap<String, PeerConfig> = config
            .peers
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        // Cancel tasks for peers removed from config or whose address changed.
        let to_remove: Vec<String> = tasks
            .iter()
            .filter(|(name, task)| new_peers.get(*name) != Some(&task.peer))
            .map(|(name, _)| name.clone())
            .collect();
        for name in to_remove {
            if let Some(task) = tasks.remove(&name) {
                info!("Removing peer session: {}", name);
                task.stop().await;
            }
        }

        // Spawn tasks for new peers (or peers whose config changed above).
        for (name, peer) in &new_peers {
            if !tasks.contains_key(name) {
                let token = CancellationToken::new();
                let handle = tokio::spawn(run_peer(
                    peer.clone(),
                    client_cfg.clone(),
                    node_name.clone(),
                    Arc::clone(&metrics),
                    token.clone(),
                ));
                tasks.insert(
                    name.clone(),
                    PeerTask {
                        peer: peer.clone(),
                        cancel: token,
                        handle,
                    },
                );
                info!("Started peer session: {}", name);
            }
        }

        tokio::select! {
            result = config_rx.changed() => { if result.is_err() { break; } }
            () = cancel.cancelled() => break,
        }
    }

    // Shutdown: cancel all running tasks.
    for task in tasks.into_values() {
        task.stop().await;
    }
}

/// Run everything this node keeps open to one peer until cancelled: the
/// primary session, and the session pool if one is configured.
async fn run_peer(
    peer: PeerConfig,
    client_cfg: ClientConfig,
    node_name: String,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let Some(pool_cfg) = client_cfg.pool.clone() else {
        run_peer_loop(peer, client_cfg, node_name, metrics, cancel).await;
        return;
    };
    let pool = crate::pool::run(crate::pool::PoolArgs {
        peer: peer.clone(),
        client_cfg: client_cfg.clone(),
        pool_cfg,
        node_name: node_name.clone(),
        metrics: Arc::clone(&metrics),
        cancel: cancel.clone(),
    });
    tokio::join!(
        run_peer_loop(peer, client_cfg, node_name, metrics, cancel),
        pool
    );
}

/// Connect to one peer and run sessions with it, reconnecting after each
/// one ends, until cancelled.
async fn run_peer_loop(
    peer: PeerConfig,
    client_cfg: ClientConfig,
    node_name: String,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    metrics.init_disconnect_labels(&node_name, &peer.name);

    loop {
        let addr = format!("{}:{}", peer.host, peer.heartbeat_port);

        let connect_result = tokio::select! {
            r = TcpStream::connect(&addr) => r,
            () = cancel.cancelled() => return,
        };

        match connect_result {
            Err(e) => {
                error!("Connect failed to {}: {}", addr, e);
                let label = DisconnectLabel {
                    node: node_name.clone(),
                    peer: peer.name.clone(),
                    reason: "connect_failed".to_owned(),
                };
                metrics.client_disconnects.get_or_create(&label).inc();
                let sleep = tokio::time::sleep(Duration::from_secs(client_cfg.reconnect_delay));
                tokio::select! {
                    () = sleep => {},
                    () = cancel.cancelled() => return,
                }
            }
            Ok(stream) => {
                let reason =
                    run_session(stream, &peer, &client_cfg, &node_name, &metrics, &cancel).await;

                if reason == "cancelled" {
                    return;
                }

                info!(
                    "Reconnecting to {} in {}s",
                    peer.name, client_cfg.reconnect_delay
                );
                let sleep = tokio::time::sleep(Duration::from_secs(client_cfg.reconnect_delay));
                tokio::select! {
                    () = sleep => {},
                    () = cancel.cancelled() => return,
                }
            }
        }
    }
}

/// Returns the disconnect reason string, or "cancelled" if cancellation was requested.
async fn run_session(
    mut stream: TcpStream,
    peer: &PeerConfig,
    client_cfg: &ClientConfig,
    node_name: &str,
    metrics: &Metrics,
    cancel: &CancellationToken,
) -> &'static str {
    // Send handshake: announce our node name.
    if let Err(e) = protocol::send_handshake(&mut stream, node_name).await {
        warn!("Handshake send failed: {}", e);
        return "local_error";
    }

    let label = PeerLabel {
        node: node_name.to_owned(),
        peer: peer.name.clone(),
    };
    let start_ts = now_f64();
    info!("Session established to {}", peer.name);

    record_session_start(metrics, &label, start_ts);

    let observer = PrimaryObserver {
        metrics,
        label: &label,
        start_ts,
        max_misses: client_cfg.max_misses,
    };
    let reason = heartbeat_loop(&mut stream, client_cfg, None, cancel, &observer).await;

    let duration = now_f64() - start_ts;
    metrics.client_session_active.get_or_create(&label).set(0);
    metrics
        .client_session_duration
        .get_or_create(&label)
        .set(duration);

    if reason != "cancelled" {
        metrics
            .client_disconnects
            .get_or_create(&DisconnectLabel {
                node: node_name.to_owned(),
                peer: peer.name.clone(),
                reason: reason.to_owned(),
            })
            .inc();
        info!(
            "Session ended peer={} duration={:.1}s reason={}",
            peer.name, duration, reason
        );
    }

    reason
}

/// Records what happens in a [`heartbeat_loop`], so primary and pool
/// sessions can share the loop but keep separate metrics.
pub(crate) trait HeartbeatObserver: Sync {
    /// A heartbeat was sent.
    fn sent(&self);
    /// An echo arrived. `rtt` is set only when it matched the heartbeat
    /// just sent; a late echo for an earlier one has no meaningful RTT.
    fn echo(&self, rtt: Option<f64>);
    /// No echo arrived in time; `consecutive` counts this run of misses.
    fn missed(&self, consecutive: i64);
}

/// Send heartbeats and wait for their echoes until the session ends.
///
/// Returns the disconnect reason, `"cancelled"` if `cancel` fired, or
/// `"expired"` once `until` is reached; the session is still healthy then.
pub(crate) async fn heartbeat_loop(
    stream: &mut TcpStream,
    client_cfg: &ClientConfig,
    until: Option<Instant>,
    cancel: &CancellationToken,
    observer: &impl HeartbeatObserver,
) -> &'static str {
    let heartbeat_interval = Duration::from_secs(client_cfg.heartbeat_interval);
    let echo_timeout = heartbeat_interval;
    let mut seq: u64 = 0;
    let mut consecutive_missed: i64 = 0;
    let mut next_send = Instant::now();
    // One reader for the whole session: the echo wait below is cut off by a
    // deadline, and the reader keeps any partly received packet across that.
    let mut echoes = protocol::HeartbeatReader::new();

    loop {
        // Wait until the next heartbeat time, or the end of the session's
        // planned life if that comes first, or cancel.
        if let Some(end) = until.filter(|end| *end <= next_send) {
            tokio::select! {
                () = tokio::time::sleep_until(end) => return "expired",
                () = cancel.cancelled() => return "cancelled",
            }
        }
        if next_send > Instant::now() {
            tokio::select! {
                () = tokio::time::sleep_until(next_send) => {},
                () = cancel.cancelled() => return "cancelled",
            }
        }
        next_send += heartbeat_interval;

        // Send heartbeat; record which seq we sent so we can validate the echo.
        let sent_seq = seq;
        let send_ts = match tokio::select! {
            r = protocol::send_heartbeat(stream, sent_seq) => r,
            () = cancel.cancelled() => return "cancelled",
        } {
            Ok(ts) => ts,
            Err(e) => return classify_io_error(&e),
        };
        observer.sent();
        seq += 1;

        // Wait for this heartbeat's echo. An echo for an earlier heartbeat that
        // arrived after its own timeout is queued ahead of it, so keep reading
        // until the matching seq or this round's deadline. Stopping at the
        // first echo would leave every later read one round behind, and RTT
        // would never be measured again for the rest of the session.
        let deadline = Instant::now() + echo_timeout;
        let mut got_echo = false;
        loop {
            let echo_result = tokio::select! {
                r = tokio::time::timeout_at(deadline, echoes.recv(stream)) => r,
                () = cancel.cancelled() => return "cancelled",
            };
            match echo_result {
                Err(_timeout) => break,
                Ok(Err(e)) => return classify_io_error(&e),
                Ok(Ok(echo)) => {
                    // Any echo, even a late one, confirms the connection is alive.
                    got_echo = true;
                    consecutive_missed = 0;
                    // RTT is only meaningful for the echo matching what we sent.
                    let matched = echo.seq == sent_seq;
                    observer.echo(matched.then(|| now_f64() - send_ts));
                    if matched {
                        break;
                    }
                }
            }
        }

        if !got_echo {
            consecutive_missed += 1;
            observer.missed(consecutive_missed);
            if consecutive_missed >= i64::from(client_cfg.max_misses) {
                return "timeout";
            }
            // After a timeout, reset the send schedule so the next heartbeat
            // fires after a full interval rather than immediately.
            next_send = Instant::now() + heartbeat_interval;
        }
    }
}

/// Records a primary session's heartbeats in the `tcp_monitor_client_*` metrics.
#[derive(Debug)]
struct PrimaryObserver<'a> {
    /// Shared metrics.
    metrics: &'a Metrics,
    /// Labels of this session's metrics.
    label: &'a PeerLabel,
    /// Wall-clock start of the session.
    start_ts: f64,
    /// `client.max_misses`, for the log line.
    max_misses: u32,
}

impl HeartbeatObserver for PrimaryObserver<'_> {
    fn sent(&self) {
        self.metrics
            .client_heartbeats_sent
            .get_or_create(self.label)
            .inc();
    }

    fn echo(&self, rtt: Option<f64>) {
        record_echo(self.metrics, self.label, self.start_ts, rtt);
    }

    fn missed(&self, consecutive: i64) {
        self.metrics
            .client_heartbeats_missed
            .get_or_create(self.label)
            .inc();
        self.metrics
            .client_consecutive_missed
            .get_or_create(self.label)
            .set(consecutive);
        warn!(
            "Heartbeat timeout peer={} ({}/{})",
            self.label.peer, consecutive, self.max_misses
        );
    }
}

/// Record the start of a session in the client metrics.
fn record_session_start(metrics: &Metrics, label: &PeerLabel, start_ts: f64) {
    metrics.client_session_active.get_or_create(label).set(1);
    metrics
        .client_session_start
        .get_or_create(label)
        .set(start_ts);
    metrics.client_sessions_total.get_or_create(label).inc();
    metrics
        .client_consecutive_missed
        .get_or_create(label)
        .set(0);
}

/// Record a received echo, with its round-trip time when it matched the
/// heartbeat just sent (no RTT for a late echo of an earlier one).
fn record_echo(metrics: &Metrics, label: &PeerLabel, start_ts: f64, rtt: Option<f64>) {
    let now_ts = now_f64();
    metrics.client_heartbeats_rx.get_or_create(label).inc();
    metrics
        .client_consecutive_missed
        .get_or_create(label)
        .set(0);
    metrics
        .client_last_heartbeat
        .get_or_create(label)
        .set(now_ts);
    metrics
        .client_session_duration
        .get_or_create(label)
        .set(now_ts - start_ts);
    if let Some(rtt) = rtt {
        metrics.client_rtt.get_or_create(label).set(rtt);
    }
}

/// Map an I/O error to the disconnect reason reported in metrics.
pub(crate) fn classify_io_error(e: &std::io::Error) -> &'static str {
    match e.kind() {
        std::io::ErrorKind::ConnectionReset => "connection_reset",
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::BrokenPipe => "remote_close",
        _ => "local_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    mod classify_io_error {
        use super::*;

        #[test]
        fn maps_reset_to_connection_reset() {
            assert_eq!(
                classify_io_error(&Error::from(ErrorKind::ConnectionReset)),
                "connection_reset"
            );
        }

        #[test]
        fn maps_eof_and_broken_pipe_to_remote_close() {
            for kind in [ErrorKind::UnexpectedEof, ErrorKind::BrokenPipe] {
                assert_eq!(classify_io_error(&Error::from(kind)), "remote_close");
            }
        }

        #[test]
        fn maps_anything_else_to_local_error() {
            assert_eq!(
                classify_io_error(&Error::from(ErrorKind::NetworkUnreachable)),
                "local_error"
            );
        }
    }
}
