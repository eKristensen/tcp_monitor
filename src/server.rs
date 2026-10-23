//! Server side: accepts peer heartbeat sessions and echoes each heartbeat,
//! and serves the Blackbox Exporter probe port.

use crate::config::{Config, ServerConfig};
use crate::metrics::{disconnect_label, Metrics, PeerLabel};
use crate::protocol::SessionKind;
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

/// Hard cap on concurrent inbound heartbeat sessions, of every kind.
/// Sized for session pools: a client with the default pool keeps about 100
/// sessions to each of its peers, so a node that 10 pooling clients connect
/// to holds about 1,000. Each session costs one socket and one small task;
/// `LimitNOFILE` in `tcp-monitor.service` must stay above this.
const MAX_CONNECTIONS: usize = 2048;

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

    // Each task runs until `cancel`. One that stops earlier has failed, and
    // returning lets main notice and exit, so the service restarts complete.
    let (task, result) = tokio::select! {
        // On shutdown the tasks finish too; check `cancel` first so that is
        // not mistaken for a failure.
        biased;
        () = cancel.cancelled() => return,
        r = heartbeat_task => ("heartbeat accept loop", r),
        r = probe_task => ("probe accept loop", r),
        r = config_task => ("config watcher", r),
    };
    match result {
        Ok(()) => error!("Server {} stopped unexpectedly", task),
        Err(e) => error!("Server {} task failed: {}", task, e),
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

/// Run one inbound connection: read the handshake, then hand the session to
/// the handler for its kind.
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    heartbeat_recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let Ok(hello) = protocol::recv_handshake(&mut stream, HANDSHAKE_TIMEOUT_SECS).await else {
        debug!("Handshake failed from {}, closing", peer_addr);
        return;
    };

    if !metrics.server_peers.try_register(&hello.name) {
        warn!(
            "Server peer limit reached ({}), rejecting peer={} addr={}",
            crate::metrics::MAX_UNIQUE_SERVER_PEERS,
            hello.name,
            peer_addr
        );
        return;
    }

    let label = PeerLabel {
        node: node_name,
        peer: hello.name,
    };
    match hello.kind {
        SessionKind::Primary => {
            run_primary(stream, peer_addr, heartbeat_recv_timeout, &label, &metrics).await;
        }
        SessionKind::Pool => {
            run_pool(
                stream,
                heartbeat_recv_timeout,
                &label,
                hello.session_id,
                &metrics,
            )
            .await;
        }
        SessionKind::Check => {
            // A check exists to be answered, not counted: the client records
            // the outcome, and its planned close says nothing about the path.
            let reason = drive_session(&mut stream, heartbeat_recv_timeout, None).await;
            debug!(
                "Check session ended: peer={} session_id={} reason={}",
                label.peer, hello.session_id, reason
            );
        }
    }
}

/// Run a primary session: echo heartbeats until it ends, recording session
/// metrics and the disconnect reason.
async fn run_primary(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    heartbeat_recv_timeout: u64,
    label: &PeerLabel,
    metrics: &Metrics,
) {
    info!("Session started: peer={} addr={}", label.peer, peer_addr);
    metrics.init_disconnect_labels(&label.node, &label.peer);

    let start_ts = now_f64();
    // Counted up and down rather than set to 1 and 0: a reconnecting client
    // can have a new session open before the server has timed out the old one.
    metrics.server_session_active.get_or_create(label).inc();
    metrics
        .server_session_start
        .get_or_create(label)
        .set(start_ts);
    metrics.server_sessions_total.get_or_create(label).inc();

    let primary = Primary {
        label,
        metrics,
        start_ts,
    };
    let reason = drive_session(&mut stream, heartbeat_recv_timeout, Some(&primary)).await;

    let duration = now_f64() - start_ts;
    metrics.server_session_active.get_or_create(label).dec();
    metrics
        .server_session_duration
        .get_or_create(label)
        .set(duration);
    metrics
        .server_disconnects
        .get_or_create(&disconnect_label(&label.node, &label.peer, reason))
        .inc();

    info!(
        "Session ended: peer={} duration={:.1}s reason={}",
        label.peer, duration, reason
    );
}

/// Run one pool session: echo heartbeats until it ends, and count how it
/// ended. The client sends a goodbye when the session reaches its planned
/// lifetime, so anything other than `expired` is an early end.
async fn run_pool(
    mut stream: TcpStream,
    heartbeat_recv_timeout: u64,
    label: &PeerLabel,
    session_id: u64,
    metrics: &Metrics,
) {
    metrics.init_server_pool_labels(&label.node, &label.peer);
    metrics
        .server_pool_sessions_active
        .get_or_create(label)
        .inc();
    let started = tokio::time::Instant::now();

    let reason = drive_session(&mut stream, heartbeat_recv_timeout, None).await;

    metrics
        .server_pool_sessions_active
        .get_or_create(label)
        .dec();
    metrics
        .server_pool_ends
        .get_or_create(&disconnect_label(&label.node, &label.peer, reason))
        .inc();
    let age = started.elapsed().as_secs();
    if reason == "expired" {
        debug!(
            "Pool session expired: peer={} session_id={} age={}s",
            label.peer, session_id, age
        );
    } else {
        info!(
            "Pool session ended early: peer={} session_id={} age={}s reason={}",
            label.peer, session_id, age, reason
        );
    }
}

/// Where [`drive_session`] records the metrics of a primary session.
#[derive(Debug)]
struct Primary<'a> {
    /// Labels of this session's metrics.
    label: &'a PeerLabel,
    /// Shared metrics.
    metrics: &'a Metrics,
    /// Wall-clock start of the session.
    start_ts: f64,
}

/// Echo heartbeats until the session ends; returns the disconnect reason.
/// A goodbye from the client ends it as `expired`. Per-heartbeat metrics
/// are recorded only for primary sessions.
async fn drive_session(
    stream: &mut TcpStream,
    heartbeat_recv_timeout: u64,
    primary: Option<&Primary<'_>>,
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
            Ok(Ok(hb)) if hb.seq == protocol::GOODBYE_SEQ => return "expired",
            Ok(Ok(hb)) => {
                if let Some(p) = primary {
                    let now = now_f64();
                    p.metrics.server_heartbeats_rx.get_or_create(p.label).inc();
                    p.metrics
                        .server_last_heartbeat
                        .get_or_create(p.label)
                        .set(now);
                    p.metrics
                        .server_session_duration
                        .get_or_create(p.label)
                        .set(now - p.start_ts);
                }

                let packet = protocol::encode_packet(hb.seq, hb.timestamp);
                if let Err(e) = stream.write_all(&packet).await {
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
