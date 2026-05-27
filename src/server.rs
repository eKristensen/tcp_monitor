use crate::metrics::{DisconnectLabel, Metrics, PeerLabel};
use crate::protocol;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub async fn run(
    bind: String,
    port: u16,
    probe_port: u16,
    recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let addr: SocketAddr = format!("{}:{}", bind, port).parse().expect("invalid bind address");
    let probe_addr: SocketAddr = format!("{}:{}", bind, probe_port).parse().expect("invalid probe address");

    let listener = TcpListener::bind(addr).await.expect("failed to bind heartbeat port");
    info!("Server listening on {}", addr);

    let probe_listener = TcpListener::bind(probe_addr).await.expect("failed to bind probe port");
    info!("Probe port listening on {}", probe_addr);

    let probe_task = tokio::spawn(run_probe_port(probe_listener));

    let heartbeat_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let node = node_name.clone();
                    let m = metrics.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, peer_addr, recv_timeout, node, m).await;
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    tokio::select! {
        _ = heartbeat_task => {},
        _ = probe_task => {},
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    recv_timeout: u64,
    node_name: String,
    metrics: Arc<Metrics>,
) {
    let peer_name = protocol::recv_handshake(&mut stream, 5).await.unwrap_or_else(|_| "unknown".to_string());
    info!("Session started: peer={} addr={}", peer_name, peer_addr);

    let label = PeerLabel { node: node_name.clone(), peer: peer_name.clone() };
    metrics.init_disconnect_labels(&node_name, &peer_name);

    let start_ts = now_f64();
    metrics.srv_session_active.get_or_create(&label).set(1);
    metrics.srv_session_start.get_or_create(&label).set(start_ts);
    metrics.srv_sessions_total.get_or_create(&label).inc();

    let reason = drive_session(&mut stream, recv_timeout, &label, &metrics, start_ts).await;

    let duration = now_f64() - start_ts;
    metrics.srv_session_active.get_or_create(&label).set(0);
    metrics.srv_session_duration.get_or_create(&label).set(duration);
    metrics.srv_disconnects.get_or_create(&DisconnectLabel {
        node: node_name.clone(),
        peer: peer_name.clone(),
        reason: reason.to_string(),
    }).inc();

    info!("Session ended: peer={} duration={:.1}s reason={}", peer_name, duration, reason);
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
            Err(_) => return "timeout",
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => return "connection_reset",
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return "remote_close",
            Ok(Err(_)) => return "local_error",
            Ok(Ok(hb)) => {
                let now = now_f64();
                metrics.srv_heartbeats_rx.get_or_create(label).inc();
                metrics.srv_last_heartbeat.get_or_create(label).set(now);
                metrics.srv_session_duration.get_or_create(label).set(now - start_ts);

                // Echo the packet back.
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

/// Probe port: accept, send banner, hold open until peer closes.
async fn run_probe_port(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let _ = stream.write_all(b"TCP-MONITOR OK\r\n").await;
                    // Drain until the client closes, so the connection isn't
                    // immediately reset (important for blackbox establishment test).
                    let mut buf = [0u8; 64];
                    loop {
                        match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
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
