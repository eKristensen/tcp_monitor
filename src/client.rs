use crate::config::{ClientConfig, PeerConfig};
use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::protocol;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::Config;

pub async fn run_manager(
    metrics: Arc<Metrics>,
    mut config_rx: watch::Receiver<Config>,
) {
    // peer_name -> (peer config used at spawn time, cancel token, task handle)
    let mut tasks: HashMap<String, (PeerConfig, CancellationToken, tokio::task::JoinHandle<()>)> =
        HashMap::new();

    loop {
        let config = config_rx.borrow_and_update().clone();
        let node_name = config.node.name.clone();
        let client_cfg = match &config.client {
            Some(c) => c.clone(),
            None => {
                // No client config; cancel any existing tasks and wait for next change.
                let drained: Vec<_> = tasks.drain().collect();
                for (name, (_, token, handle)) in drained {
                    info!("Removing peer session (no [client] config): {}", name);
                    token.cancel();
                    let _ = handle.await;
                }
                if config_rx.changed().await.is_err() {
                    break;
                }
                continue;
            }
        };

        let new_peers: HashMap<String, PeerConfig> = config
            .peers
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();

        // Cancel tasks for peers removed from config or whose address changed.
        let to_remove: Vec<String> = tasks
            .iter()
            .filter(|(name, (old_peer, _, _))| {
                new_peers.get(*name) != Some(old_peer)
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in to_remove {
            if let Some((_, token, handle)) = tasks.remove(&name) {
                info!("Removing peer session: {}", name);
                token.cancel();
                let _ = handle.await;
            }
        }

        // Spawn tasks for new peers (or peers whose config changed above).
        for (name, peer) in &new_peers {
            if !tasks.contains_key(name) {
                let token = CancellationToken::new();
                let handle = tokio::spawn(run_peer_loop(
                    peer.clone(),
                    client_cfg.clone(),
                    node_name.clone(),
                    metrics.clone(),
                    token.clone(),
                ));
                tasks.insert(name.clone(), (peer.clone(), token, handle));
                info!("Started peer session: {}", name);
            }
        }

        if config_rx.changed().await.is_err() {
            break;
        }
    }

    // Shutdown: cancel all running tasks.
    for (_, (_, token, handle)) in tasks {
        token.cancel();
        let _ = handle.await;
    }
}

async fn run_peer_loop(
    peer: PeerConfig,
    client_cfg: ClientConfig,
    node_name: String,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    metrics.init_disconnect_labels(&node_name, &peer.name);

    loop {
        let addr = format!("{}:{}", peer.host, peer.port);

        let connect_result = tokio::select! {
            r = TcpStream::connect(&addr) => r,
            _ = cancel.cancelled() => return,
        };

        match connect_result {
            Err(e) => {
                error!("Connect failed to {}: {}", addr, e);
                let label = DisconnectLabel {
                    node: node_name.clone(),
                    peer: peer.name.clone(),
                    reason: "connect_failed".to_string(),
                };
                metrics.client_disconnects.get_or_create(&label).inc();
                let sleep = tokio::time::sleep(Duration::from_secs(client_cfg.reconnect_delay));
                tokio::select! {
                    _ = sleep => {},
                    _ = cancel.cancelled() => return,
                }
                continue;
            }
            Ok(stream) => {
                let reason = run_session(
                    stream,
                    &peer,
                    &client_cfg,
                    &node_name,
                    &metrics,
                    &cancel,
                )
                .await;

                if reason == "cancelled" {
                    return;
                }

                info!("Reconnecting to {} in {}s", peer.name, client_cfg.reconnect_delay);
                let sleep = tokio::time::sleep(Duration::from_secs(client_cfg.reconnect_delay));
                tokio::select! {
                    _ = sleep => {},
                    _ = cancel.cancelled() => return,
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

    let label = PeerLabel { node: node_name.to_string(), peer: peer.name.clone() };
    let start_ts = now_f64();
    info!("Session established to {}", peer.name);

    metrics.client_session_active.get_or_create(&label).set(1);
    metrics.client_session_start.get_or_create(&label).set(start_ts);
    metrics.client_sessions_total.get_or_create(&label).inc();
    metrics.client_consecutive_missed.get_or_create(&label).set(0);

    let heartbeat_interval = Duration::from_secs(client_cfg.heartbeat_interval);
    let echo_timeout = heartbeat_interval;
    let mut seq: u64 = 0;
    let mut consecutive_missed: i64 = 0;
    let mut next_send = tokio::time::Instant::now();

    let reason: &'static str = 'session: loop {
        // Wait until next heartbeat time, or cancel.
        let now = tokio::time::Instant::now();
        if next_send > now {
            tokio::select! {
                _ = tokio::time::sleep_until(next_send) => {},
                _ = cancel.cancelled() => break 'session "cancelled",
            }
        }
        next_send += heartbeat_interval;

        // Send heartbeat.
        let send_ts = match tokio::select! {
            r = protocol::send_heartbeat(&mut stream, seq) => r,
            _ = cancel.cancelled() => break 'session "cancelled",
        } {
            Ok(ts) => ts,
            Err(e) => break 'session classify_io_error(&e),
        };
        metrics.client_heartbeats_sent.get_or_create(&label).inc();
        seq += 1;

        // Wait for echo.
        let echo_result = tokio::select! {
            r = tokio::time::timeout(echo_timeout, protocol::recv_heartbeat(&mut stream)) => r,
            _ = cancel.cancelled() => break 'session "cancelled",
        };

        match echo_result {
            Err(_timeout) => {
                consecutive_missed += 1;
                metrics.client_heartbeats_missed.get_or_create(&label).inc();
                metrics.client_consecutive_missed.get_or_create(&label).set(consecutive_missed);
                warn!(
                    "Heartbeat timeout peer={} ({}/{})",
                    peer.name, consecutive_missed, client_cfg.max_misses
                );
                if consecutive_missed >= client_cfg.max_misses as i64 {
                    break 'session "timeout";
                }
            }
            Ok(Err(e)) => break 'session classify_io_error(&e),
            Ok(Ok(_echo)) => {
                let rtt = now_f64() - send_ts;
                consecutive_missed = 0;
                let now_ts = now_f64();
                metrics.client_heartbeats_rx.get_or_create(&label).inc();
                metrics.client_consecutive_missed.get_or_create(&label).set(0);
                metrics.client_rtt.get_or_create(&label).set(rtt);
                metrics.client_last_heartbeat.get_or_create(&label).set(now_ts);
                metrics.client_session_duration.get_or_create(&label).set(now_ts - start_ts);
            }
        }
    };

    let duration = now_f64() - start_ts;
    metrics.client_session_active.get_or_create(&label).set(0);
    metrics.client_session_duration.get_or_create(&label).set(duration);

    if reason != "cancelled" {
        metrics.client_disconnects.get_or_create(&DisconnectLabel {
            node: node_name.to_string(),
            peer: peer.name.clone(),
            reason: reason.to_string(),
        }).inc();
        info!("Session ended peer={} duration={:.1}s reason={}", peer.name, duration, reason);
    }

    reason
}

fn classify_io_error(e: &std::io::Error) -> &'static str {
    match e.kind() {
        std::io::ErrorKind::ConnectionReset => "connection_reset",
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::BrokenPipe => "remote_close",
        _ => "local_error",
    }
}

fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
