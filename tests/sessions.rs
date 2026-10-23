//! End-to-end tests over loopback: a real server, real client sessions, and
//! the metrics they produce. Every listener binds port 0, so tests never
//! collide with each other or with a running tcp-monitor.

// This crate only exists under `cargo test`. Saying so lets clippy treat the
// shared helpers as test code too (clippy.toml's test exemptions).
#![cfg(test)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tcp_monitor::config::{ClientConfig, Config, NodeConfig, PeerConfig, PoolConfig, ServerConfig};
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
        bind: "127.0.0.1".to_owned(),
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
            name: name.to_owned(),
        },
        server,
        client: Some(ClientConfig {
            heartbeat_interval: 1,
            max_misses: 3,
            reconnect_delay: 1,
            pool: None,
        }),
        peers,
    }
}

fn peer(name: &str, addr: SocketAddr) -> PeerConfig {
    PeerConfig {
        name: name.to_owned(),
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
        node_name: name.to_owned(),
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
    start_client_with(node_config(name, server_config(10), peers))
}

/// A client whose peers each get a pool of `lifetime / open_interval` sessions.
fn start_pool_client(
    name: &str,
    peers: Vec<PeerConfig>,
    open_interval: u64,
    lifetime: u64,
) -> TestClient {
    let mut config = node_config(name, server_config(10), peers);
    if let Some(client) = config.client.as_mut() {
        client.pool = Some(PoolConfig {
            open_interval,
            lifetime,
        });
    }
    start_client_with(config)
}

fn start_client_with(config: Config) -> TestClient {
    let metrics = Metrics::new();
    let (config_tx, config_rx) = watch::channel(config);
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
    sample(&metrics.encode().unwrap(), selector)
}

/// Like [`metric`], but reads from an already-encoded snapshot, so several
/// values can be compared as of the same instant.
fn sample(text: &str, selector: &str) -> Option<f64> {
    text.lines()
        .find(|line| line.starts_with(selector))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

/// Sum of every sample whose line starts with `prefix` and contains `part`,
/// e.g. all reasons of one result.
fn sum_where(metrics: &Metrics, prefix: &str, part: &str) -> f64 {
    metrics
        .encode()
        .unwrap()
        .lines()
        .filter(|line| line.starts_with(prefix) && line.contains(part))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Predicate for [`wait_for_metric`]: the value is exactly `n` (a count).
fn exactly(n: f64) -> impl Fn(f64) -> bool + Send + Sync {
    move |v| (v - n).abs() < f64::EPSILON
}

/// Poll `metric` until `predicate` holds, failing the test after [`WAIT`].
async fn wait_for_metric(
    metrics: &Metrics,
    selector: &str,
    predicate: impl Fn(f64) -> bool + Send + Sync,
) {
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

/// Connect to the server and complete a version 2 handshake as `name`.
async fn connect_v2(server: &TestServer, name: &str, kind: protocol::SessionKind) -> TcpStream {
    let mut stream = TcpStream::connect(server.heartbeat_addr).await.unwrap();
    protocol::send_handshake_v2(&mut stream, name, kind, 1)
        .await
        .unwrap();
    stream
}

/// Send one heartbeat and wait for its echo.
async fn ping(stream: &mut TcpStream) {
    protocol::send_heartbeat(stream, 0).await.unwrap();
    within("echo", protocol::recv_heartbeat(stream))
        .await
        .unwrap();
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

        stream.write_all(&0_u32.to_be_bytes()).await.unwrap();
        let mut buf = [0_u8; 1];
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
            #[expect(
                clippy::collection_is_never_read,
                reason = "holds each session open so it times out instead of closing"
            )]
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
    async fn counts_concurrent_primary_sessions_from_one_peer() {
        let server = start_server("srv", 10).await;
        let mut first = connect_as(&server, "cli").await;
        let mut second = connect_as(&server, "cli").await;
        ping(&mut first).await;
        ping(&mut second).await;
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            exactly(2.0),
        )
        .await;

        // The old session ending must not hide the new one still running.
        drop(first);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_disconnects_total{node="srv",peer="cli",reason="remote_close"}"#,
            |v| v >= 1.0,
        )
        .await;
        assert_eq!(
            metric(
                &server.metrics,
                r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#
            ),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn records_a_pool_session_closed_with_goodbye_as_expired() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_v2(&server, "cli", protocol::SessionKind::Pool).await;
        ping(&mut stream).await;

        protocol::send_goodbye(&mut stream).await.unwrap();
        drop(stream);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="expired"}"#,
            |v| v >= 1.0,
        )
        .await;
        assert_eq!(
            metric(
                &server.metrics,
                r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="remote_close"}"#
            ),
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn records_a_pool_session_closed_without_goodbye_as_an_early_end() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_v2(&server, "cli", protocol::SessionKind::Pool).await;
        ping(&mut stream).await;

        drop(stream);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="remote_close"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn keeps_pool_sessions_out_of_the_primary_session_metrics() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_v2(&server, "cli", protocol::SessionKind::Pool).await;
        ping(&mut stream).await;

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_sessions_active{node="srv",peer="cli"}"#,
            exactly(1.0),
        )
        .await;
        assert!(metric(&server.metrics, "tcp_monitor_server_sessions_total{").is_none());
    }

    #[tokio::test]
    async fn echoes_check_sessions_without_counting_them() {
        let server = start_server("srv", 10).await;
        let mut stream = connect_v2(&server, "cli", protocol::SessionKind::Check).await;

        ping(&mut stream).await;

        let text = server.metrics.encode().unwrap();
        assert!(
            !text.contains(r#"peer="cli""#),
            "check was counted:\n{text}"
        );
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
        let mut banner = vec![0_u8; expected.len()];
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
            // Read and discard until the client goes away; how it goes away
            // (FIN, RST, or error) does not matter to this test.
            let mut sink = Vec::new();
            drop(stream.read_to_end(&mut sink).await);
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
    async fn keeps_measuring_rtt_after_one_late_echo() {
        // A peer that answers the first heartbeat after the client's 1 s echo
        // timeout, then answers every later one at once. The late echo sits
        // in the client's receive buffer when the next round starts.
        let slow_once = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = slow_once.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            let (mut stream, _) = slow_once.accept().await.unwrap();
            protocol::recv_handshake(&mut stream, 5).await.unwrap();
            let mut first = true;
            while let Ok(hb) = protocol::recv_heartbeat(&mut stream).await {
                if first {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    first = false;
                }
                let echo = protocol::encode_packet(hb.seq, hb.timestamp);
                if stream.write_all(&echo).await.is_err() {
                    return;
                }
            }
        });

        let client = start_client("cli", vec![peer("slow", addr)]);

        // The first echo really was late...
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_heartbeats_missed_total{node="cli",peer="slow"}"#,
            |v| v >= 1.0,
        )
        .await;
        // ...and RTT is still measured on the prompt echoes after it.
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_heartbeat_rtt_seconds{node="cli",peer="slow"}"#,
            |v| v > 0.0,
        )
        .await;
    }

    #[tokio::test]
    async fn measures_rtt_in_the_round_after_an_echo_split_across_the_timeout() {
        // A peer that sends the first 10 bytes of the first echo, waits past
        // the client's 1 s echo timeout, then sends the remaining 6 and
        // answers every later heartbeat at once. The timeout fires mid-packet.
        let split_once = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = split_once.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            let (mut stream, _) = split_once.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            protocol::recv_handshake(&mut stream, 5).await.unwrap();
            let mut first = true;
            while let Ok(hb) = protocol::recv_heartbeat(&mut stream).await {
                let echo = protocol::encode_packet(hb.seq, hb.timestamp);
                let (head, tail) = echo.split_at(10);
                if first {
                    first = false;
                    if stream.write_all(head).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    if stream.write_all(tail).await.is_err() {
                        return;
                    }
                } else if stream.write_all(&echo).await.is_err() {
                    return;
                }
            }
        });

        let client = start_client("cli", vec![peer("split", addr)]);

        // Round 1 times out mid-packet (1 sent). In round 2 (2 sent) the
        // client must finish the split echo, see it is stale, then match the
        // prompt echo and measure RTT. A client that dropped the 10 buffered
        // bytes would instead read a misaligned, garbage packet in round 2,
        // and measure RTT only in round 3. Both values come from one snapshot.
        let sent_when_rtt_first_measured = within("an RTT measurement", async {
            loop {
                let text = client.metrics.encode().unwrap();
                let rtt = sample(
                    &text,
                    r#"tcp_monitor_client_heartbeat_rtt_seconds{node="cli",peer="split"}"#,
                );
                if rtt.is_some_and(|v| v > 0.0) {
                    return sample(
                        &text,
                        r#"tcp_monitor_client_heartbeats_sent_total{node="cli",peer="split"}"#,
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;

        assert_eq!(sent_when_rtt_first_measured, Some(2.0));
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

mod client_pool {
    use super::*;

    /// A peer that echoes heartbeats, but cuts every pool session after its
    /// first echo, like a load balancer path dying under pinned sessions.
    /// Primary and check sessions are echoed normally.
    async fn start_pool_killer() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let Ok(hello) = protocol::recv_handshake(&mut stream, 5).await else {
                        return;
                    };
                    while let Ok(hb) = protocol::recv_heartbeat(&mut stream).await {
                        let echo = protocol::encode_packet(hb.seq, hb.timestamp);
                        if stream.write_all(&echo).await.is_err()
                            || hello.kind == protocol::SessionKind::Pool
                        {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn fills_the_pool_at_startup() {
        let server = start_server("srv", 10).await;

        // 60 s lifetime / 20 s interval: 3 sessions, none of which expire
        // during the test.
        let client = start_pool_client("cli", vec![peer("srv", server.heartbeat_addr)], 20, 60);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_sessions_active{node="srv",peer="cli"}"#,
            exactly(3.0),
        )
        .await;
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_sessions_active{node="cli",peer="srv"}"#,
            exactly(3.0),
        )
        .await;
    }

    #[tokio::test]
    async fn keeps_the_primary_session_alongside_the_pool() {
        let server = start_server("srv", 10).await;

        let _client = start_pool_client("cli", vec![peer("srv", server.heartbeat_addr)], 20, 60);

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_session_active{node="srv",peer="cli"}"#,
            exactly(1.0),
        )
        .await;
    }

    #[tokio::test]
    async fn closes_sessions_at_their_planned_lifetime_as_expired_on_both_sides() {
        let server = start_server("srv", 10).await;

        // Lifetimes of 1-3 s, so sessions expire within the test.
        let client = start_pool_client("cli", vec![peer("srv", server.heartbeat_addr)], 1, 2);

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_session_ends_total{node="cli",peer="srv",reason="expired"}"#,
            |v| v >= 2.0,
        )
        .await;
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="expired"}"#,
            |v| v >= 2.0,
        )
        .await;
        let early = sum_where(
            &server.metrics,
            "tcp_monitor_server_pool_session_ends_total{",
            "",
        ) - metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="expired"}"#,
        )
        .unwrap();
        assert!(early == 0.0, "server saw {early} unplanned pool closes");
    }

    #[tokio::test]
    async fn says_goodbye_to_every_session_when_stopped() {
        let server = start_server("srv", 10).await;
        let client = start_pool_client("cli", vec![peer("srv", server.heartbeat_addr)], 20, 60);
        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_sessions_active{node="srv",peer="cli"}"#,
            exactly(3.0),
        )
        .await;

        client.cancel.cancel();
        within("client shutdown", client.handle).await.unwrap();

        wait_for_metric(
            &server.metrics,
            r#"tcp_monitor_server_pool_session_ends_total{node="srv",peer="cli",reason="expired"}"#,
            exactly(3.0),
        )
        .await;
    }

    #[tokio::test]
    async fn checks_a_new_session_after_an_early_end_and_finds_it_works() {
        let addr = start_pool_killer().await;

        let client = start_pool_client("cli", vec![peer("lb", addr)], 20, 60);

        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_session_ends_total{node="cli",peer="lb",reason="remote_close"}"#,
            |v| v >= 1.0,
        )
        .await;
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_new_session_checks_total{node="cli",peer="lb",reason="remote_close",result="ok"}"#,
            |v| v >= 1.0,
        )
        .await;
    }

    #[tokio::test]
    async fn counts_sessions_that_ended_early_as_planned_but_not_active() {
        let addr = start_pool_killer().await;

        let client = start_pool_client("cli", vec![peer("lb", addr)], 20, 60);

        // All 3 sessions die after one echo; their slots stay planned until
        // their planned end, a minute away.
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_sessions_planned{node="cli",peer="lb"}"#,
            exactly(3.0),
        )
        .await;
        wait_for_metric(
            &client.metrics,
            r#"tcp_monitor_client_pool_sessions_active{node="cli",peer="lb"}"#,
            |v| v == 0.0,
        )
        .await;
    }

    #[tokio::test]
    async fn records_a_failed_check_when_the_peer_is_gone() {
        // Echoes once on the pool session, then closes it and stops
        // listening, so the check that follows cannot connect. The primary
        // session, which may arrive first, is just held open.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            #[expect(
                clippy::collection_is_never_read,
                reason = "holds other sessions open so only the pool session ends"
            )]
            let mut others = Vec::new();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let hello = protocol::recv_handshake(&mut stream, 5).await.unwrap();
                if hello.kind != protocol::SessionKind::Pool {
                    others.push(stream);
                    continue;
                }
                drop(listener);
                let hb = protocol::recv_heartbeat(&mut stream).await.unwrap();
                let echo = protocol::encode_packet(hb.seq, hb.timestamp);
                drop(stream.write_all(&echo).await);
                return;
            }
        });

        let client = start_pool_client("cli", vec![peer("gone", addr)], 60, 60);

        let prefix = "tcp_monitor_client_pool_new_session_checks_total{";
        within("a failed check", async {
            loop {
                if sum_where(&client.metrics, prefix, r#"result="failed""#) >= 1.0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let ok = sum_where(&client.metrics, prefix, r#"result="ok""#);
        assert!(ok < 1.0, "{ok} checks succeeded");
    }
}
