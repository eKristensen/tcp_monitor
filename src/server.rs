//! Server side: accepts peer heartbeat sessions and echoes each heartbeat,
//! and serves the Blackbox Exporter probe port.

use crate::config::{Config, ServerConfig};
use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::{now_f64, protocol};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Hard cap on concurrent inbound heartbeat sessions.
const MAX_CONNECTIONS: usize = 128;

/// Separate, smaller cap for probe connections (health-check only).
const MAX_PROBE_CONNECTIONS: usize = 32;

/// Seconds a new heartbeat connection has to send its handshake.
const HANDSHAKE_TIMEOUT_SECS: u64 = 5;

/// Banner sent on every probe connection; the Blackbox Exporter module matches it.
const PROBE_BANNER: &[u8] = b"TCP-MONITOR OK\r\n";

/// The bound heartbeat and probe sockets.
///
/// Binding is separate from [`run`] so startup can fail cleanly on a port
/// conflict, and so tests can bind port 0 and read back the chosen port.
#[derive(Debug)]
pub struct Listeners {
    /// Listener on `server.heartbeat_port`.
    pub heartbeat: TcpListener,
    /// Listener on `server.probe_port`.
    pub probe: TcpListener,
}

impl Listeners {
    /// Bind the heartbeat and probe ports from the server config.
    ///
    /// # Errors
    /// Returns an error naming the port that could not be bound.
    pub async fn bind(cfg: &ServerConfig) -> io::Result<Self> {
        let heartbeat = bind_one(&cfg.bind, cfg.heartbeat_port, "heartbeat_port").await?;
        let probe = bind_one(&cfg.bind, cfg.probe_port, "probe_port").await?;
        Ok(Self { heartbeat, probe })
    }
}

/// Bind one listener; the error names the config field (`what`) and address.
async fn bind_one(host: &str, port: u16, what: &str) -> io::Result<TcpListener> {
    let addr = crate::config::bind_addr(host, port)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    TcpListener::bind(addr)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("bind {what} {addr}: {e}")))
}

/// Everything [`run`] needs.
#[derive(Debug)]
pub struct ServerArgs {
    /// Already-bound heartbeat and probe listeners.
    pub listeners: Listeners,
    /// Server config at startup; reloads are compared against it.
    pub config: ServerConfig,
    /// This node's name, the `node` label on server metrics.
    pub node_name: String,
    /// Shared metrics.
    pub metrics: Arc<Metrics>,
    /// Stops the accept loops and the config watcher.
    pub cancel: CancellationToken,
    /// Reloaded configs (SIGHUP); timeouts are applied to new connections.
    pub config_rx: watch::Receiver<Config>,
}

/// Run the heartbeat and probe accept loops until `cancel` fires.
pub async fn run(args: ServerArgs) {
    let ServerArgs {
        listeners,
        config,
        node_name,
        metrics,
        cancel,
        config_rx,
    } = args;

    for (what, listener) in [
        ("Server", &listeners.heartbeat),
        ("Probe port", &listeners.probe),
    ] {
        if let Ok(addr) = listener.local_addr() {
            info!("{what} listening on {addr}");
        }
    }

    // Arc<AtomicU64> lets the config-watch task update these values on SIGHUP
    // while the accept loops read them on each new connection — no mutex needed
    // for a single-value load/store. Already-running sessions keep their value.
    let heartbeat_recv_timeout = Arc::new(AtomicU64::new(config.heartbeat_recv_timeout));
    let probe_idle_timeout = Arc::new(AtomicU64::new(config.probe_idle_timeout));

    let heartbeat_task = tokio::spawn(heartbeat_accept_loop(
        listeners.heartbeat,
        Arc::clone(&heartbeat_recv_timeout),
        node_name,
        metrics,
        Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        cancel.clone(),
    ));

    let probe_task = tokio::spawn(probe_accept_loop(
        listeners.probe,
        Arc::new(Semaphore::new(MAX_PROBE_CONNECTIONS)),
        Arc::clone(&probe_idle_timeout),
        cancel.clone(),
    ));

    let config_task = tokio::spawn(watch_config(
        config,
        config_rx,
        heartbeat_recv_timeout,
        probe_idle_timeout,
        cancel.clone(),
    ));

    tokio::select! {
        _ = heartbeat_task => {},
        _ = probe_task => {},
        _ = config_task => {},
        () = cancel.cancelled() => {},
    }
}

/// React to timeout changes from SIGHUP reloads.
/// `bind`/`heartbeat_port`/`probe_port` changes require a restart — rebinding a live port is disruptive.
async fn watch_config(
    initial: ServerConfig,
    mut config_rx: watch::Receiver<Config>,
    heartbeat_recv_timeout: Arc<AtomicU64>,
    probe_idle_timeout: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            result = config_rx.changed() => {
                if result.is_err() { break; }
                let srv = config_rx.borrow_and_update().server.clone();
                if srv.bind != initial.bind || srv.heartbeat_port != initial.heartbeat_port {
                    warn!(
                        "server.bind/heartbeat_port changed ({}:{} → {}:{}); restart required",
                        initial.bind, initial.heartbeat_port, srv.bind, srv.heartbeat_port
                    );
                }
                if srv.probe_port != initial.probe_port {
                    warn!(
                        "server.probe_port changed ({} → {}); restart required",
                        initial.probe_port, srv.probe_port
                    );
                }
                heartbeat_recv_timeout.store(srv.heartbeat_recv_timeout, Ordering::Relaxed);
                probe_idle_timeout.store(srv.probe_idle_timeout, Ordering::Relaxed);
                info!(
                    "Server timeouts updated: heartbeat_recv={}s probe_idle={}s",
                    srv.heartbeat_recv_timeout, srv.probe_idle_timeout
                );
            }
            () = cancel.cancelled() => break,
        }
    }
}

/// Accept heartbeat connections and spawn a session task for each, up to
/// [`MAX_CONNECTIONS`], until cancelled.
async fn heartbeat_accept_loop(
    listener: TcpListener,
    heartbeat_recv_timeout: Arc<AtomicU64>,
    node_name: String,
    metrics: Arc<Metrics>,
    sem: Arc<Semaphore>,
    cancel: CancellationToken,
) {
    loop {
        let accept = tokio::select! {
            r = listener.accept() => r,
            () = cancel.cancelled() => return,
        };
        match accept {
            Ok((stream, peer_addr)) => {
                let Ok(permit) = Arc::clone(&sem).try_acquire_owned() else {
                    warn!(
                        "Connection limit ({}) reached, dropping {}",
                        MAX_CONNECTIONS, peer_addr
                    );
                    continue;
                };
                let node_name = node_name.clone();
                let metrics = Arc::clone(&metrics);
                let heartbeat_recv_timeout = heartbeat_recv_timeout.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_connection(
                        stream,
                        peer_addr,
                        heartbeat_recv_timeout,
                        node_name,
                        metrics,
                    )
                    .await;
                });
            }
            Err(e) => {
                error!("Accept error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Run one inbound session: handshake, then echo heartbeats until it ends,
/// recording session metrics and the disconnect reason.
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    heartbeat_recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let Ok(peer_name) = protocol::recv_handshake(&mut stream, HANDSHAKE_TIMEOUT_SECS).await else {
        debug!("Handshake failed from {}, closing", peer_addr);
        return;
    };

    if !metrics.server_peers.try_register(&peer_name) {
        warn!(
            "Server peer limit reached ({}), rejecting peer={} addr={}",
            crate::metrics::MAX_UNIQUE_SERVER_PEERS,
            peer_name,
            peer_addr
        );
        return;
    }

    info!("Session started: peer={} addr={}", peer_name, peer_addr);

    let label = PeerLabel {
        node: node_name.clone(),
        peer: peer_name.clone(),
    };
    metrics.init_disconnect_labels(&node_name, &peer_name);

    let start_ts = now_f64();
    metrics.server_session_active.get_or_create(&label).set(1);
    metrics
        .server_session_start
        .get_or_create(&label)
        .set(start_ts);
    metrics.server_sessions_total.get_or_create(&label).inc();

    let reason = drive_session(
        &mut stream,
        heartbeat_recv_timeout,
        &label,
        &metrics,
        start_ts,
    )
    .await;

    let duration = now_f64() - start_ts;
    metrics.server_session_active.get_or_create(&label).set(0);
    metrics
        .server_session_duration
        .get_or_create(&label)
        .set(duration);
    metrics
        .server_disconnects
        .get_or_create(&DisconnectLabel {
            node: node_name,
            peer: peer_name.clone(),
            reason: reason.to_owned(),
        })
        .inc();

    info!(
        "Session ended: peer={} duration={:.1}s reason={}",
        peer_name, duration, reason
    );
}

/// Echo heartbeats until the session ends; returns the disconnect reason.
async fn drive_session(
    stream: &mut TcpStream,
    heartbeat_recv_timeout: u64,
    label: &PeerLabel,
    metrics: &Metrics,
    start_ts: f64,
) -> &'static str {
    loop {
        let result = tokio::time::timeout(
            Duration::from_secs(heartbeat_recv_timeout),
            protocol::recv_heartbeat(stream),
        )
        .await;

        match result {
            Err(_timeout) => return "timeout",
            Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionReset => return "connection_reset",
            Ok(Err(e))
                if e.kind() == io::ErrorKind::UnexpectedEof
                    || e.kind() == io::ErrorKind::BrokenPipe =>
            {
                return "remote_close"
            }
            Ok(Err(_)) => return "local_error",
            Ok(Ok(hb)) => {
                let now = now_f64();
                metrics.server_heartbeats_rx.get_or_create(label).inc();
                metrics.server_last_heartbeat.get_or_create(label).set(now);
                metrics
                    .server_session_duration
                    .get_or_create(label)
                    .set(now - start_ts);

                let packet = protocol::encode_packet(hb.seq, hb.timestamp);
                if let Err(e) = stream.write_all(&packet).await {
                    #[expect(
                        clippy::wildcard_enum_match_arm,
                        reason = "io::ErrorKind is non-exhaustive with ~40 variants; every kind not listed is deliberately local_error"
                    )]
                    match e.kind() {
                        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => {
                            return "connection_reset"
                        }
                        _ => {
                            warn!("Echo write error: {}", e);
                            return "local_error";
                        }
                    }
                }
            }
        }
    }
}

/// Probe port: accept → send banner → drain until peer closes (or idle timeout).
/// Used by the Prometheus Blackbox Exporter for TCP establishment and response tests.
async fn probe_accept_loop(
    listener: TcpListener,
    sem: Arc<Semaphore>,
    probe_idle_timeout: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    loop {
        let accept = tokio::select! {
            r = listener.accept() => r,
            () = cancel.cancelled() => return,
        };
        match accept {
            Ok((mut stream, addr)) => {
                let Ok(permit) = Arc::clone(&sem).try_acquire_owned() else {
                    warn!("Probe connection limit reached, dropping {}", addr);
                    continue;
                };
                let idle_secs = probe_idle_timeout.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = stream.write_all(PROBE_BANNER).await {
                        debug!("Probe banner write to {} failed: {}", addr, e);
                        return;
                    }
                    // Drain until the client closes; timeout prevents permit exhaustion
                    // from clients that never close.
                    let mut buf = [0_u8; 64];
                    let drain = async {
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                    };
                    if tokio::time::timeout(Duration::from_secs(idle_secs), drain)
                        .await
                        .is_err()
                    {
                        debug!(
                            "Probe connection idle for {}s, dropping: {}",
                            idle_secs, addr
                        );
                    } else {
                        debug!("Probe connection closed: {}", addr);
                    }
                });
            }
            Err(e) => {
                error!("Probe accept error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
