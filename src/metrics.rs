use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PeerLabel {
    pub node: String,
    pub peer: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DisconnectLabel {
    pub node: String,
    pub peer: String,
    pub reason: String,
}

pub struct Metrics {
    // --- server side ---
    pub srv_session_active: Family<PeerLabel, Gauge>,
    pub srv_session_start: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub srv_session_duration: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub srv_sessions_total: Family<PeerLabel, Counter>,
    pub srv_heartbeats_rx: Family<PeerLabel, Counter>,
    pub srv_last_heartbeat: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub srv_disconnects: Family<DisconnectLabel, Counter>,

    // --- client side ---
    pub cli_session_active: Family<PeerLabel, Gauge>,
    pub cli_session_start: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub cli_session_duration: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub cli_sessions_total: Family<PeerLabel, Counter>,
    pub cli_heartbeats_sent: Family<PeerLabel, Counter>,
    pub cli_heartbeats_rx: Family<PeerLabel, Counter>,
    pub cli_heartbeats_missed: Family<PeerLabel, Counter>,
    pub cli_consecutive_missed: Family<PeerLabel, Gauge>,
    pub cli_rtt: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub cli_last_heartbeat: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub cli_disconnects: Family<DisconnectLabel, Counter>,

    pub registry: Registry,
}

impl Metrics {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();

        macro_rules! reg {
            ($name:literal, $help:literal) => {{
                let f = Family::default();
                registry.register($name, $help, f.clone());
                f
            }};
        }

        Arc::new(Metrics {
            srv_session_active: reg!("tcp_monitor_server_session_active",
                "1 if a server session is currently active with this peer"),
            srv_session_start: reg!("tcp_monitor_server_session_start_timestamp_seconds",
                "Unix timestamp when the current/last server session was established"),
            srv_session_duration: reg!("tcp_monitor_server_session_duration_seconds",
                "Duration of the active or last completed server session"),
            srv_sessions_total: reg!("tcp_monitor_server_sessions",
                "Total inbound sessions accepted since startup"),
            srv_heartbeats_rx: reg!("tcp_monitor_server_heartbeats_received",
                "Total heartbeat packets received from a peer"),
            srv_last_heartbeat: reg!("tcp_monitor_server_last_heartbeat_timestamp_seconds",
                "Unix timestamp of the most recently received heartbeat"),
            srv_disconnects: reg!("tcp_monitor_server_session_disconnects",
                "Server session disconnects by peer and reason"),

            cli_session_active: reg!("tcp_monitor_client_session_active",
                "1 if a client session is currently active to this peer"),
            cli_session_start: reg!("tcp_monitor_client_session_start_timestamp_seconds",
                "Unix timestamp when the current/last client session was established"),
            cli_session_duration: reg!("tcp_monitor_client_session_duration_seconds",
                "Duration of the active or last completed client session"),
            cli_sessions_total: reg!("tcp_monitor_client_sessions",
                "Total outbound sessions established since startup"),
            cli_heartbeats_sent: reg!("tcp_monitor_client_heartbeats_sent",
                "Total heartbeat packets sent to a peer"),
            cli_heartbeats_rx: reg!("tcp_monitor_client_heartbeats_received",
                "Total heartbeat echoes received from a peer"),
            cli_heartbeats_missed: reg!("tcp_monitor_client_heartbeats_missed",
                "Total heartbeat echoes not received within the timeout window"),
            cli_consecutive_missed: reg!("tcp_monitor_client_heartbeats_consecutive_missed",
                "Current run of consecutive unanswered heartbeats"),
            cli_rtt: reg!("tcp_monitor_client_heartbeat_rtt_seconds",
                "Round-trip time of the most recent heartbeat echo"),
            cli_last_heartbeat: reg!("tcp_monitor_client_last_heartbeat_timestamp_seconds",
                "Unix timestamp of the most recent successful heartbeat echo"),
            cli_disconnects: reg!("tcp_monitor_client_session_disconnects",
                "Client session disconnects by peer and reason"),

            registry,
        })
    }

    /// Pre-create all disconnect reason label combinations so they appear in
    /// /metrics from startup rather than only after the first disconnect.
    pub fn init_disconnect_labels(&self, node: &str, peer: &str) {
        for reason in &["remote_close", "connection_reset", "timeout", "local_error"] {
            let label = DisconnectLabel {
                node: node.to_string(),
                peer: peer.to_string(),
                reason: reason.to_string(),
            };
            let _ = self.srv_disconnects.get_or_create(&label);
        }
        for reason in &["remote_close", "connection_reset", "timeout", "local_error", "connect_failed"] {
            let label = DisconnectLabel {
                node: node.to_string(),
                peer: peer.to_string(),
                reason: reason.to_string(),
            };
            let _ = self.cli_disconnects.get_or_create(&label);
        }
    }
}
