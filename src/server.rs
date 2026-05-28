use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::protocol;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Hard cap on concurrent inbound heartbeat sessions.
const MAX_CONNECTIONS: usize = 128;

/// Separate, smaller cap for probe connections (health-check only).
const MAX_PROBE_CONNECTIONS: usize = 32;

pub struct ServerArgs {
    pub bind: String,
    pub port: u16,
    pub probe_port: u16,
    pub heartbeat_recv_timeout: u64,
    pub probe_idle_timeout: u64,
    pub node_name: String,
    pub metrics: Arc<Metrics>,
    pub cancel: CancellationToken,
    pub config_rx: tokio::sync::watch::Receiver<crate::config::Config>,
}

pub async fn run(args: ServerArgs) {
    let ServerArgs {
        bind,
        port,
        probe_port,
        heartbeat_recv_timeout: initial_heartbeat_recv_timeout,
        probe_idle_timeout: initial_probe_idle_timeout,
        node_name,
        metrics,
        cancel,
        mut config_rx,
    } = args;
    let addr: SocketAddr = format!("{}:{}", bind, port)
        .parse()
        .expect("invalid bind address");
    let probe_addr: SocketAddr = format!("{}:{}", bind, probe_port)
        .parse()
        .expect("invalid probe bind address");

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind heartbeat port");
    info!("Server listening on {}", addr);

    let probe_listener = TcpListener::bind(probe_addr)
        .await
        .expect("failed to bind probe port");
    info!("Probe port listening on {}", probe_addr);

    // Atomics shared with accept loops so SIGHUP reloads take effect for new sessions.
    let heartbeat_recv_timeout = Arc::new(AtomicU64::new(initial_heartbeat_recv_timeout));
    let probe_idle_timeout = Arc::new(AtomicU64::new(initial_probe_idle_timeout));

    let heartbeat_sem = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let probe_sem = Arc::new(Semaphore::new(MAX_PROBE_CONNECTIONS));

    let heartbeat_task = {
        let node_name = node_name.clone();
        let metrics = metrics.clone();
        let heartbeat_sem = heartbeat_sem.clone();
        let cancel = cancel.clone();
        let heartbeat_recv_timeout = heartbeat_recv_timeout.clone();
        tokio::spawn(async move {
            heartbeat_accept_loop(
                listener,
                heartbeat_recv_timeout,
                node_name,
                metrics,
                heartbeat_sem,
                cancel,
            )
            .await;
        })
    };

    let probe_task = {
        let cancel = cancel.clone();
        let probe_idle_timeout = probe_idle_timeout.clone();
        tokio::spawn(probe_accept_loop(probe_listener, probe_sem, probe_idle_timeout, cancel))
    };

    // React to timeout changes from SIGHUP reloads.
    // bind/port/probe_port changes require a restart — rebinding a live port is disruptive.
    let config_task = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = config_rx.changed() => {
                        if result.is_err() { break; }
                        let srv = config_rx.borrow_and_update().server.clone();
                        if srv.bind != bind || srv.port != port {
                            warn!(
                                "server.bind/port changed ({}:{} → {}:{}); restart required",
                                bind, port, srv.bind, srv.port
                            );
                        }
                        if srv.probe_port != probe_port {
                            warn!(
                                "server.probe_port changed ({} → {}); restart required",
                                probe_port, srv.probe_port
                            );
                        }
                        heartbeat_recv_timeout
                            .store(srv.heartbeat_recv_timeout, Ordering::Relaxed);
                        probe_idle_timeout.store(srv.probe_idle_timeout, Ordering::Relaxed);
                        info!(
                            "Server timeouts updated: heartbeat_recv={}s probe_idle={}s",
                            srv.heartbeat_recv_timeout, srv.probe_idle_timeout
                        );
                    }
                    _ = cancel.cancelled() => break,
                }
            }
        })
    };

    tokio::select! {
        _ = heartbeat_task => {},
        _ = probe_task => {},
        _ = config_task => {},
        _ = cancel.cancelled() => {},
    }
}

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
            _ = cancel.cancelled() => return,
        };
        match accept {
            Ok((stream, peer_addr)) => {
                let permit = match Arc::clone(&sem).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!(
                            "Connection limit ({}) reached, dropping {}",
                            MAX_CONNECTIONS, peer_addr
                        );
                        continue;
                    }
                };
                let node_name = node_name.clone();
                let metrics = metrics.clone();
                let heartbeat_recv_timeout = heartbeat_recv_timeout.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_connection(stream, peer_addr, heartbeat_recv_timeout, node_name, metrics)
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

async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    heartbeat_recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let peer_name = match protocol::recv_handshake(&mut stream, 5).await {
        Ok(name) => name,
        Err(_) => {
            debug!("Handshake failed from {}, closing", peer_addr);
            return;
        }
    };

    if !metrics.try_register_server_peer(&peer_name) {
        warn!(
            "Server peer limit reached ({}), rejecting peer={} addr={}",
            crate::metrics::MAX_UNIQUE_SERVER_PEERS, peer_name, peer_addr
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
    metrics.server_session_start.get_or_create(&label).set(start_ts);
    metrics.server_sessions_total.get_or_create(&label).inc();

    let reason =
        drive_session(&mut stream, heartbeat_recv_timeout, &label, &metrics, start_ts).await;

    let duration = now_f64() - start_ts;
    metrics.server_session_active.get_or_create(&label).set(0);
    metrics.server_session_duration.get_or_create(&label).set(duration);
    metrics
        .server_disconnects
        .get_or_create(&DisconnectLabel {
            node: node_name,
            peer: peer_name.clone(),
            reason: reason.to_string(),
        })
        .inc();

    info!(
        "Session ended: peer={} duration={:.1}s reason={}",
        peer_name, duration, reason
    );
}

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
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                return "connection_reset"
            }
            Ok(Err(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::BrokenPipe =>
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
                if let Err(e) = tokio::io::AsyncWriteExt::write_all(stream, &packet).await {
                    match e.kind() {
                        std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset => return "connection_reset",
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
            _ = cancel.cancelled() => return,
        };
        match accept {
            Ok((mut stream, addr)) => {
                let permit = match Arc::clone(&sem).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("Probe connection limit reached, dropping {}", addr);
                        continue;
                    }
                };
                let idle_secs = probe_idle_timeout.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let _ = stream.write_all(b"TCP-MONITOR OK\r\n").await;
                    // Drain until the client closes; timeout prevents permit exhaustion
                    // from clients that never close.
                    let mut buf = [0u8; 64];
                    let drain = async {
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                    };
                    let _ = tokio::time::timeout(Duration::from_secs(idle_secs), drain).await;
                    debug!("Probe connection closed: {}", addr);
                });
            }
            Err(e) => {
                error!("Probe accept error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
