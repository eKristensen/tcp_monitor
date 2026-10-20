//! End-to-end tests over loopback: a real server, real client sessions, and
//! the metrics they produce. Every listener binds port 0, so tests never
//! collide with each other or with a running tcp-monitor.

// clippy.toml exempts `#[test]` functions; the shared helpers below need the
// same exemption, since a failed unwrap there is equally a failed test.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test helpers: panicking is how a test fails"
)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tcp_monitor::config::{ClientConfig, Config, NodeConfig, PeerConfig, ServerConfig};
use tcp_monitor::metrics::Metrics;
use tcp_monitor::{client, protocol, server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Upper bound for any condition a test waits on.
const WAIT: Duration = Duration::from_secs(10);

fn server_config(heartbeat_recv_timeout: u64) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        heartbeat_port: 0,
        metrics_port: 0,
        probe_port: 0,
        heartbeat_recv_timeout,
        probe_idle_timeout: 5,
    }
}

fn node_config(name: &str, server: ServerConfig, peers: Vec<PeerConfig>) -> Config {
    Config {
        node: NodeConfig {
            name: name.to_string(),
        },
        server,
        client: Some(ClientConfig {
            heartbeat_interval: 1,
            max_misses: 3,
            reconnect_delay: 1,
        }),
        peers,
    }
}

fn peer(name: &str, addr: SocketAddr) -> PeerConfig {
    PeerConfig {
        name: name.to_string(),
        host: addr.ip().to_string(),
        heartbeat_port: addr.port(),
    }
}

/// A running server plus the handles a test needs to inspect and stop it.
struct TestServer {
    heartbeat_addr: SocketAddr,
    probe_addr: SocketAddr,
    metrics: Arc<Metrics>,
    config_tx: watch::Sender<Config>,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

async fn start_server(name: &str, heartbeat_recv_timeout: u64) -> TestServer {
    let srv_cfg = server_config(heartbeat_recv_timeout);
    let listeners = server::Listeners::bind(&srv_cfg).await.unwrap();
    let heartbeat_addr = listeners.heartbeat.local_addr().unwrap();
    let probe_addr = listeners.probe.local_addr().unwrap();
    let metrics = Metrics::new();
    let (config_tx, config_rx) = watch::channel(node_config(name, srv_cfg.clone(), vec![]));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(server::run(server::ServerArgs {
        listeners,
        config: srv_cfg,
        node_name: name.to_string(),
        metrics: Arc::clone(&metrics),
        cancel: cancel.clone(),
        config_rx,
    }));
    TestServer {
        heartbeat_addr,
        probe_addr,
        metrics,
        config_tx,
        cancel,
        handle,
    }
}

/// A running client manager.
struct TestClient {
    metrics: Arc<Metrics>,
    config_tx: watch::Sender<Config>,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

fn start_client(name: &str, peers: Vec<PeerConfig>) -> TestClient {
    let metrics = Metrics::new();
    let (config_tx, config_rx) = watch::channel(node_config(name, server_config(10), peers));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(client::run_manager(
        Arc::clone(&metrics),
        config_rx,
        cancel.clone(),
    ));
    TestClient {
        metrics,
        config_tx,
        cancel,
        handle,
    }
}

/// Value of the sample whose name and labels start with `selector`, e.g.
/// `tcp_monitor_server_sessions_total{node="srv",peer="cli"`.
fn metric(metrics: &Metrics, selector: &str) -> Option<f64> {
    metrics
        .encode()
        .unwrap()
        .lines()
        .find(|line| line.starts_with(selector))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

/// Poll `metric` until `predicate` holds, failing the test after [`WAIT`].
async fn wait_for_metric(metrics: &Metrics, selector: &str, predicate: impl Fn(f64) -> bool) {
    let result = tokio::time::timeout(WAIT, async {
        loop {
            if metric(metrics, selector).is_some_and(&predicate) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "timed out waiting on {selector}; last value: {:?}",
        metric(metrics, selector)
    );
}

async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"))
}

/// Connect to the server and complete the handshake as `name`.
async fn connect_as(server: &TestServer, name: &str) -> TcpStream {
    let mut stream = TcpStream::connect(server.heartbeat_addr).await.unwrap();
    protocol::send_handshake(&mut stream, name).await.unwrap();
    stream
}

/// A port on 127.0.0.1 with nothing listening on it.
async fn closed_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

mod server_sessions {
    use super::*;

    #[tokio::test]
    async fn echoes_each_heartbeat_unchanged() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_as(&server, "cli").await;

        let sent = protocol::encode_packet(7, 1234.5);
        stream.write_all(&sent).await.unwrap();
        let echo = within("echo", protocol::recv_heartbeat(&mut stream))
            .await
            .unwrap();

        assert_eq!(protocol::encode_packet(echo.seq, echo.timestamp), sent);
    }

    #[tokio::test]
    async fn labels_session_metrics_with_the_handshake_name() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_as(&server, "cli").await;

        protocol::send_heartbeat(&mut stream, 0).await.unwrap();

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_heartbeats_received_total{node="srv",peer="cli"}"#,
            |v| v >= 1.0,
        )
        .await;
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn records_remote_close_when_client_disconnects() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_as(&server, "cli").await;
        protocol::send_heartbeat(&mut stream, 0).await.unwrap();
        protocol::recv_heartbeat(&mut stream).await.unwrap();

        drop(stream);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_disconnects_total{node="srv",peer="cli",reason="remote_close"}"#,
            |v| v >= 1.0,
        )
        .await;
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v == 0.0,
        )
        .await;
    }

    #[tokio::test]
    async fn records_timeout_when_client_goes_silent() {
        let server = start_server("srv", 1).await;
        let _stream = connect_as(&server, "cli").await;

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_disconnects_total{node="srv",peer="cli",reason="timeout"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn closes_connection_without_a_session_on_invalid_handshake() {
        let server = start_server("srv", 10).await;
        let mut stream = TcpStream::connect(server.heartbeat_addr).await.unwrap();

        stream.write_all(&0u32.to_be_bytes()).await.unwrap();
        let mut buf = [0u8; 1];
        let read = within("server close", stream.read(&mut buf)).await;

        assert!(matches!(read, Ok(0) | Err(_)), "connection left open");
        assert!(metric(&server.metrics, "tcp_monitor_server_sessions_total{").is_none());
    }

    #[tokio::test]
    async fn applies_reloaded_heartbeat_recv_timeout_to_new_sessions() {
        let server = start_server("srv", 600).await;
        let mut reloaded = server.config_tx.borrow().clone();
        reloaded.server.heartbeat_recv_timeout = 1;
        server.config_tx.send(reloaded).unwrap();

        // The server applies the reload asynchronously, so keep opening silent
        // sessions until one times out. Only a session opened after the reload
        // can: the others got the 600 s timeout and are held open meanwhile.
        within("a session to pick up the reloaded timeout", async {
            let mut sessions = Vec::new();
            loop {
                sessions.push(connect_as(&server, "cli").await);
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let timeouts = metric(
                    &server.metrics,
                    r#"tcp_monitor_server_session_disconnects_total{node="srv",peer="cli",reason="timeout"}"#,
                );
                if timeouts.is_some_and(|v| v >= 1.0) {
                    return;
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn run_returns_promptly_when_cancelled() {
        let server = start_server("srv", 10).await;
        let _stream = connect_as(&server, "cli").await;

        server.cancel.cancel();

        within("server shutdown", server.handle).await.unwrap();
    }
}

mod probe_port {
    use super::*;

    #[tokio::test]
    async fn sends_the_banner_on_connect() {
        let server = start_server("srv", 10).await;
        let mut stream = TcpStream::connect(server.probe_addr).await.unwrap();

        // Literal on purpose: blackbox.example.yml matches this exact banner.
        let expected = b"TCP-MONITOR OK\r\n";
        let mut banner = vec![0u8; expected.len()];
        within("banner", stream.read_exact(&mut banner))
            .await
            .unwrap();

        assert_eq!(banner, expected);
    }
}

mod listeners {
    use super::*;

    #[tokio::test]
    async fn bind_names_the_port_that_is_already_in_use() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = server_config(10);
        cfg.heartbeat_port = taken.local_addr().unwrap().port();

        let err = server::Listeners::bind(&cfg).await.unwrap_err();

        assert!(err.to_string().contains("heartbeat_port"), "{err}");
    }
}

mod client_sessions {
    use super::*;

    #[tokio::test]
    async fn measures_rtt_against_a_live_server() {
        let server = start_server("srv", 10).await;

        let client = start_client("cli", vec![peer("srv", server.heartbeat_addr)]);

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_heartbeats_received_total{node="cli",peer="srv"}"#,
            |v| v >= 1.0,
        )
        .await;
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_heartbeat_rtt_seconds{node="cli",peer="srv"}"#,
            |v| v > 0.0,
        )
        .await;
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_heartbeats_received_total{node="srv",peer="cli"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn records_timeout_after_max_misses_when_server_never_echoes() {
        // A peer that completes the handshake and then never answers: the
        // silent-network-drop case the monitor exists to detect.
        let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = silent.local_addr().unwrap();
        let _held = tokio::spawn(async move {
            let (mut stream, _) = silent.accept().await.unwrap();
            protocol::recv_handshake(&mut stream, 5).await.unwrap();
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink).await;
        });

        let client = start_client("cli", vec![peer("mute", addr)]);

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_session_disconnects_total{node="cli",peer="mute",reason="timeout"}"#,
            |v| v >= 1.0,
        )
        .await;
        assert_eq!(
            metric(
                &client.metrics,
                r#"tcp_monitor_client_heartbeats_missed_total{node="cli",peer="mute"}"#
            ),
            Some(3.0)
        );
    }

    #[tokio::test]
    async fn records_connect_failed_when_nothing_listens() {
        let addr = closed_port().await;

        let client = start_client("cli", vec![peer("gone", addr)]);

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_session_disconnects_total{node="cli",peer="gone",reason="connect_failed"}"#,
            |v| v >= 1.0,
        )
        .await;
    }
}

mod client_reload {
    use super::*;

    #[tokio::test]
    async fn starts_a_session_for_a_peer_added_on_reload() {
        let server = start_server("srv", 10).await;
        let client = start_client("cli", vec![]);

        let mut reloaded = client.config_tx.borrow().clone();
        reloaded.peers = vec![peer("srv", server.heartbeat_addr)];
        client.config_tx.send(reloaded).unwrap();

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_session_active{node="cli",peer="srv"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn stops_the_session_for_a_peer_removed_on_reload() {
        let server = start_server("srv", 10).await;
        let client = start_client("cli", vec![peer("srv", server.heartbeat_addr)]);
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v >= 1.0,
        )
        .await;

        let mut reloaded = client.config_tx.borrow().clone();
        reloaded.peers.clear();
        client.config_tx.send(reloaded).unwrap();

        // Only "the session ended" is asserted, not the server-side reason:
        // the client's close is usually a FIN (remote_close), but an RST
        // (connection_reset) if an unread echo is still buffered.
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v == 0.0,
        )
        .await;
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_session_active{node="cli",peer="srv"}"#,
            |v| v == 0.0,
        )
        .await;
    }

    #[tokio::test]
    async fn stops_all_sessions_when_client_section_is_removed() {
        let server = start_server("srv", 10).await;
        let client = start_client("cli", vec![peer("srv", server.heartbeat_addr)]);
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v >= 1.0,
        )
        .await;

        let mut reloaded = client.config_tx.borrow().clone();
        reloaded.client = None;
        client.config_tx.send(reloaded).unwrap();

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            |v| v == 0.0,
        )
        .await;
    }

    #[tokio::test]
    async fn run_manager_returns_promptly_when_cancelled() {
        let server = start_server("srv", 10).await;
        let client = start_client("cli", vec![peer("srv", server.heartbeat_addr)]);
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_session_active{node="cli",peer="srv"}"#,
            |v| v >= 1.0,
        )
        .await;

        client.cancel.cancel();

        within("client shutdown", client.handle).await.unwrap();
    }
}
