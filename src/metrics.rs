//! Prometheus metric families and the registry they are exposed from.

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

/// Hard cap on distinct inbound peer names that may allocate metric label entries.
/// prometheus-client has no removal API, so entries are permanent; this bounds growth.
pub const MAX_UNIQUE_SERVER_PEERS: usize = 512;

/// Disconnect reasons a server session can end with.
pub const SERVER_DISCONNECT_REASONS: [&str; 4] =
    ["remote_close", "connection_reset", "timeout", "local_error"];

/// Disconnect reasons a client session can end with.
pub const CLIENT_DISCONNECT_REASONS: [&str; 5] = [
    "remote_close",
    "connection_reset",
    "timeout",
    "local_error",
    "connect_failed",
];

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

#[derive(Debug)]
pub struct Metrics {
    // --- server side ---
    pub server_session_active: Family<PeerLabel, Gauge>,
    pub server_session_start: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub server_session_duration: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub server_sessions_total: Family<PeerLabel, Counter>,
    pub server_heartbeats_rx: Family<PeerLabel, Counter>,
    pub server_last_heartbeat: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub server_disconnects: Family<DisconnectLabel, Counter>,

    // --- client side ---
    pub client_session_active: Family<PeerLabel, Gauge>,
    pub client_session_start: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub client_session_duration: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub client_sessions_total: Family<PeerLabel, Counter>,
    pub client_heartbeats_sent: Family<PeerLabel, Counter>,
    pub client_heartbeats_rx: Family<PeerLabel, Counter>,
    pub client_heartbeats_missed: Family<PeerLabel, Counter>,
    pub client_consecutive_missed: Family<PeerLabel, Gauge>,
    pub client_rtt: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub client_last_heartbeat: Family<PeerLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    pub client_disconnects: Family<DisconnectLabel, Counter>,

    pub registry: Registry,

    seen_server_peers: Mutex<HashSet<String>>,
}

impl Metrics {
    /// Create all metric families and register them.
    /// Returned in an `Arc` because every task shares the same instance.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();

        macro_rules! reg {
            ($name:literal, $help:literal) => {{
                let f = Family::default();
                registry.register($name, $help, f.clone());
                f
            }};
        }

        Arc::new(Self {
            server_session_active: reg!(
                "tcp_monitor_server_session_active",
                "1 if a server session is currently active with this peer"
            ),
            server_session_start: reg!(
                "tcp_monitor_server_session_start_timestamp_seconds",
                "Unix timestamp when the current/last server session was established"
            ),
            server_session_duration: reg!(
                "tcp_monitor_server_session_duration_seconds",
                "Duration of the active or last completed server session"
            ),
            server_sessions_total: reg!(
                "tcp_monitor_server_sessions",
                "Total inbound sessions accepted since startup"
            ),
            server_heartbeats_rx: reg!(
                "tcp_monitor_server_heartbeats_received",
                "Total heartbeat packets received from a peer"
            ),
            server_last_heartbeat: reg!(
                "tcp_monitor_server_last_heartbeat_timestamp_seconds",
                "Unix timestamp of the most recently received heartbeat"
            ),
            server_disconnects: reg!(
                "tcp_monitor_server_session_disconnects",
                "Server session disconnects by peer and reason"
            ),

            client_session_active: reg!(
                "tcp_monitor_client_session_active",
                "1 if a client session is currently active to this peer"
            ),
            client_session_start: reg!(
                "tcp_monitor_client_session_start_timestamp_seconds",
                "Unix timestamp when the current/last client session was established"
            ),
            client_session_duration: reg!(
                "tcp_monitor_client_session_duration_seconds",
                "Duration of the active or last completed client session"
            ),
            client_sessions_total: reg!(
                "tcp_monitor_client_sessions",
                "Total outbound sessions established since startup"
            ),
            client_heartbeats_sent: reg!(
                "tcp_monitor_client_heartbeats_sent",
                "Total heartbeat packets sent to a peer"
            ),
            client_heartbeats_rx: reg!(
                "tcp_monitor_client_heartbeats_received",
                "Total heartbeat echoes received from a peer"
            ),
            client_heartbeats_missed: reg!(
                "tcp_monitor_client_heartbeats_missed",
                "Total heartbeat echoes not received within the timeout window"
            ),
            client_consecutive_missed: reg!(
                "tcp_monitor_client_heartbeats_consecutive_missed",
                "Current run of consecutive unanswered heartbeats"
            ),
            client_rtt: reg!(
                "tcp_monitor_client_heartbeat_rtt_seconds",
                "Round-trip time of the most recent heartbeat echo"
            ),
            client_last_heartbeat: reg!(
                "tcp_monitor_client_last_heartbeat_timestamp_seconds",
                "Unix timestamp of the most recent successful heartbeat echo"
            ),
            client_disconnects: reg!(
                "tcp_monitor_client_session_disconnects",
                "Client session disconnects by peer and reason"
            ),

            registry,
            seen_server_peers: Mutex::new(HashSet::new()),
        })
    }

    /// Returns true if the peer may proceed (either already known or within the cap).
    /// Returns false when the cap is reached; the caller should reject the connection.
    pub fn try_register_server_peer(&self, peer: &str) -> bool {
        // The set is always left consistent, so a panic elsewhere while holding
        // the lock does not invalidate it.
        let mut seen = self
            .seen_server_peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if seen.contains(peer) {
            return true;
        }
        if seen.len() >= MAX_UNIQUE_SERVER_PEERS {
            return false;
        }
        seen.insert(peer.to_string());
        true
    }

    /// Render all metrics in the Prometheus text exposition format.
    ///
    /// # Errors
    /// Returns an error if a metric fails to encode.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &self.registry)?;
        Ok(buf)
    }

    /// Pre-create all disconnect reason label combinations so they appear in
    /// /metrics from startup rather than only after the first disconnect.
    pub fn init_disconnect_labels(&self, node: &str, peer: &str) {
        for reason in SERVER_DISCONNECT_REASONS {
            let label = DisconnectLabel {
                node: node.to_string(),
                peer: peer.to_string(),
                reason: reason.to_string(),
            };
            let _ = self.server_disconnects.get_or_create(&label);
        }
        for reason in CLIENT_DISCONNECT_REASONS {
            let label = DisconnectLabel {
                node: node.to_string(),
                peer: peer.to_string(),
                reason: reason.to_string(),
            };
            let _ = self.client_disconnects.get_or_create(&label);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod try_register_server_peer {
        use super::*;

        #[test]
        fn accepts_new_peers_up_to_the_cap() {
            let metrics = Metrics::new();

            let accepted = (0..MAX_UNIQUE_SERVER_PEERS)
                .all(|i| metrics.try_register_server_peer(&format!("p{i}")));

            assert!(accepted);
        }

        #[test]
        fn rejects_a_new_peer_once_the_cap_is_reached() {
            let metrics = Metrics::new();
            for i in 0..MAX_UNIQUE_SERVER_PEERS {
                metrics.try_register_server_peer(&format!("p{i}"));
            }

            assert!(!metrics.try_register_server_peer("one-too-many"));
        }

        #[test]
        fn still_accepts_a_known_peer_once_the_cap_is_reached() {
            let metrics = Metrics::new();
            for i in 0..MAX_UNIQUE_SERVER_PEERS {
                metrics.try_register_server_peer(&format!("p{i}"));
            }

            assert!(metrics.try_register_server_peer("p0"));
        }
    }

    mod init_disconnect_labels {
        use super::*;

        #[test]
        fn exposes_every_reason_at_zero_before_any_disconnect() {
            let metrics = Metrics::new();

            metrics.init_disconnect_labels("me", "them");

            // Literal reason lists: these are the documented values in the
            // README "Disconnect reasons" table that dashboards depend on.
            let text = metrics.encode().unwrap();
            let expected = [
                ("server", "remote_close"),
                ("server", "connection_reset"),
                ("server", "timeout"),
                ("server", "local_error"),
                ("client", "remote_close"),
                ("client", "connection_reset"),
                ("client", "timeout"),
                ("client", "local_error"),
                ("client", "connect_failed"),
            ];
            for (side, reason) in expected {
                let line = format!(
                    "tcp_monitor_{side}_session_disconnects_total{{node=\"me\",peer=\"them\",reason=\"{reason}\"}} 0"
                );
                assert!(text.contains(&line), "missing: {line}");
            }
        }
    }

    mod encode {
        use super::*;

        /// Metric names are a public interface (dashboards, alerts, README).
        /// Renaming one must be a deliberate change to this list.
        #[test]
        fn exposes_documented_metric_names() {
            let metrics = Metrics::new();
            let label = PeerLabel {
                node: "me".to_string(),
                peer: "them".to_string(),
            };
            metrics.init_disconnect_labels("me", "them");
            let _ = metrics.server_session_active.get_or_create(&label);
            let _ = metrics.server_session_start.get_or_create(&label);
            let _ = metrics.server_session_duration.get_or_create(&label);
            let _ = metrics.server_sessions_total.get_or_create(&label);
            let _ = metrics.server_heartbeats_rx.get_or_create(&label);
            let _ = metrics.server_last_heartbeat.get_or_create(&label);
            let _ = metrics.client_session_active.get_or_create(&label);
            let _ = metrics.client_session_start.get_or_create(&label);
            let _ = metrics.client_session_duration.get_or_create(&label);
            let _ = metrics.client_sessions_total.get_or_create(&label);
            let _ = metrics.client_heartbeats_sent.get_or_create(&label);
            let _ = metrics.client_heartbeats_rx.get_or_create(&label);
            let _ = metrics.client_heartbeats_missed.get_or_create(&label);
            let _ = metrics.client_consecutive_missed.get_or_create(&label);
            let _ = metrics.client_rtt.get_or_create(&label);
            let _ = metrics.client_last_heartbeat.get_or_create(&label);

            let text = metrics.encode().unwrap();

            for name in [
                "tcp_monitor_server_session_active",
                "tcp_monitor_server_session_start_timestamp_seconds",
                "tcp_monitor_server_session_duration_seconds",
                "tcp_monitor_server_sessions_total",
                "tcp_monitor_server_heartbeats_received_total",
                "tcp_monitor_server_last_heartbeat_timestamp_seconds",
                "tcp_monitor_server_session_disconnects_total",
                "tcp_monitor_client_session_active",
                "tcp_monitor_client_session_start_timestamp_seconds",
                "tcp_monitor_client_session_duration_seconds",
                "tcp_monitor_client_sessions_total",
                "tcp_monitor_client_heartbeats_sent_total",
                "tcp_monitor_client_heartbeats_received_total",
                "tcp_monitor_client_heartbeats_missed_total",
                "tcp_monitor_client_heartbeats_consecutive_missed",
                "tcp_monitor_client_heartbeat_rtt_seconds",
                "tcp_monitor_client_last_heartbeat_timestamp_seconds",
                "tcp_monitor_client_session_disconnects_total",
            ] {
                let sample = format!("\n{name}{{node=\"me\",peer=\"them\"");
                assert!(text.contains(&sample), "missing metric: {name}");
            }
        }
    }
}
