use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::protocol;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// Hard cap on concurrent inbound heartbeat sessions.
/// Protects against resource exhaustion from connection floods.
const MAX_CONNECTIONS: usize = 128;

pub async fn run(
    bind: String,
    port: u16,
    probe_port: u16,
    recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
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

    let sem = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    let heartbeat_task = {
        let node = node_name.clone();
        let m = metrics.clone();
        let s = sem.clone();
        tokio::spawn(async move {
            heartbeat_accept_loop(listener, recv_timeout, node, m, s).await;
        })
    };

    let probe_task = tokio::spawn(probe_accept_loop(probe_listener, sem));

    tokio::select! {
        _ = heartbeat_task => {},
        _ = probe_task => {},
    }
}

async fn heartbeat_accept_loop(
    listener: TcpListener,
    recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
    sem: Arc<Semaphore>,
) {
    loop {
        match listener.accept().await {
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
                let node = node_name.clone();
                let m = metrics.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_connection(stream, peer_addr, recv_timeout, node, m).await;
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
    recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let peer_name = protocol::recv_handshake(&mut stream, 5)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    info!("Session started: peer={} addr={}", peer_name, peer_addr);

    let label = PeerLabel {
        node: node_name.clone(),
        peer: peer_name.clone(),
    };
    metrics.init_disconnect_labels(&node_name, &peer_name);

    let start_ts = now_f64();
    metrics.srv_session_active.get_or_create(&label).set(1);
    metrics.srv_session_start.get_or_create(&label).set(start_ts);
    metrics.srv_sessions_total.get_or_create(&label).inc();

    let reason = drive_session(&mut stream, recv_timeout, &label, &metrics, start_ts).await;

    let duration = now_f64() - start_ts;
    metrics.srv_session_active.get_or_create(&label).set(0);
    metrics.srv_session_duration.get_or_create(&label).set(duration);
    metrics
        .srv_disconnects
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
    recv_timeout: u64,
    label: &PeerLabel,
    metrics: &Metrics,
    start_ts: f64,
) -> &'static str {
    loop {
        let result = tokio::time::timeout(
            Duration::from_secs(recv_timeout),
            protocol::recv_heartbeat(stream),
        )
        .await;

        match result {
            Err(_timeout) => return "timeout",
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                return "connection_reset"
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return "remote_close",
            Ok(Err(_)) => return "local_error",
            Ok(Ok(hb)) => {
                let now = now_f64();
                metrics.srv_heartbeats_rx.get_or_create(label).inc();
                metrics.srv_last_heartbeat.get_or_create(label).set(now);
                metrics
                    .srv_session_duration
                    .get_or_create(label)
                    .set(now - start_ts);

                let mut buf = [0u8; protocol::PACKET_SIZE];
                buf[..8].copy_from_slice(&hb.seq.to_be_bytes());
                buf[8..].copy_from_slice(&hb.timestamp.to_bits().to_be_bytes());
                if let Err(e) = tokio::io::AsyncWriteExt::write_all(stream, &buf).await {
                    warn!("Echo write error: {}", e);
                    return "local_error";
                }
            }
        }
    }
}

/// Probe port: accept → send banner → hold until peer closes.
/// Used by the Prometheus Blackbox Exporter for TCP establishment and response tests.
async fn probe_accept_loop(listener: TcpListener, sem: Arc<Semaphore>) {
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                let permit = match Arc::clone(&sem).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("Probe connection limit reached, dropping {}", addr);
                        continue;
                    }
                };
                tokio::spawn(async move {
                    let _permit = permit;
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let _ = stream.write_all(b"TCP-MONITOR OK\r\n").await;
                    // Drain until the client closes so the connection isn't reset immediately.
                    let mut buf = [0u8; 64];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                    info!("Probe connection closed: {}", addr);
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
