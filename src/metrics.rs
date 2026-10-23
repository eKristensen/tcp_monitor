//! Prometheus metric families and the registry they are exposed from.

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
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

/// Reasons a server-side pool session can end with. `expired` means the
/// client closed it on purpose at the end of its planned lifetime.
pub const SERVER_POOL_END_REASONS: [&str; 5] = [
    "expired",
    "remote_close",
    "connection_reset",
    "timeout",
    "local_error",
];

/// Reasons a client-side pool session can end with. Every reason except
/// `expired` is an early end.
pub const CLIENT_POOL_END_REASONS: [&str; 6] = [
    "expired",
    "remote_close",
    "connection_reset",
    "timeout",
    "local_error",
    "connect_failed",
];

/// Early ends of an established pool session, each followed by a check of
/// whether a brand-new session works.
pub const POOL_EARLY_END_REASONS: [&str; 4] =
    ["remote_close", "connection_reset", "timeout", "local_error"];

/// Outcomes of that check.
pub const CHECK_RESULTS: [&str; 2] = ["ok", "failed"];

/// Upper bounds, in seconds, for pool session ages.
///
/// Used as the `age_at_end` histogram buckets and as the `le` label on the
/// by-age gauges. Chosen to be readable in dashboards (1m … 36h) and to cover
/// the default one-day lifetime and its ±50% spread. Changing them changes
/// the series dashboards and alerts select.
pub const POOL_AGE_BUCKETS: [u32; 13] = [
    60, 300, 900, 1_800, 3_600, 7_200, 14_400, 28_800, 43_200, 64_800, 86_400, 108_000, 129_600,
];

/// Histogram buckets for pool heartbeat round-trip time, in seconds:
/// 0.5 ms (same rack) up to 5 s (far beyond any healthy path).
const RTT_BUCKETS: [f64; 13] = [
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Builds a histogram with [`POOL_AGE_BUCKETS`].
fn age_histogram() -> Histogram {
    Histogram::new(POOL_AGE_BUCKETS.into_iter().map(f64::from))
}

/// Builds a histogram with [`RTT_BUCKETS`].
fn rtt_histogram() -> Histogram {
    Histogram::new(RTT_BUCKETS.into_iter())
}

/// Constructor for a histogram family; a plain `fn` so the family type can be named.
pub type HistogramConstructor = fn() -> Histogram;

/// Labels on per-session metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PeerLabel {
    /// This node's name (`node.name`).
    pub node: String,
    /// The other end of the session.
    pub peer: String,
}

/// Labels on disconnect counters.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DisconnectLabel {
    /// This node's name (`node.name`).
    pub node: String,
    /// The other end of the session.
    pub peer: String,
    /// One of [`SERVER_DISCONNECT_REASONS`] or [`CLIENT_DISCONNECT_REASONS`].
    pub reason: String,
}

/// Labels on the pool's by-age gauges.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct AgeLabel {
    /// This node's name (`node.name`).
    pub node: String,
    /// The other end of the session.
    pub peer: String,
    /// Upper age bound in seconds, from [`POOL_AGE_BUCKETS`], or `+Inf`.
    /// Cumulative like a histogram bucket, so Grafana can draw it as a heatmap.
    pub le: String,
}

/// Labels on the new-session checks that follow an early pool session end.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CheckLabel {
    /// This node's name (`node.name`).
    pub node: String,
    /// The other end of the session.
    pub peer: String,
    /// How the pool session ended, one of [`POOL_EARLY_END_REASONS`].
    pub reason: String,
    /// Whether a new session worked, one of [`CHECK_RESULTS`].
    pub result: String,
}

/// Gauge holding an `f64` (timestamps, durations, RTT).
pub type FloatGauge = Gauge<f64, AtomicU64>;

/// Every metric family, and the registry they are exposed from.
/// Each field's doc names the metric it exports (counters get `_total`).
#[derive(Debug)]
pub struct Metrics {
    /// `tcp_monitor_server_session_active`
    pub server_session_active: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_server_session_start_timestamp_seconds`
    pub server_session_start: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_server_session_duration_seconds`
    pub server_session_duration: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_server_sessions_total`
    pub server_sessions_total: Family<PeerLabel, Counter>,
    /// `tcp_monitor_server_heartbeats_received_total`
    pub server_heartbeats_rx: Family<PeerLabel, Counter>,
    /// `tcp_monitor_server_last_heartbeat_timestamp_seconds`
    pub server_last_heartbeat: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_server_session_disconnects_total`
    pub server_disconnects: Family<DisconnectLabel, Counter>,
    /// `tcp_monitor_server_pool_sessions_active`
    pub server_pool_sessions_active: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_server_pool_session_ends_total`
    pub server_pool_ends: Family<DisconnectLabel, Counter>,

    /// `tcp_monitor_client_session_active`
    pub client_session_active: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_client_session_start_timestamp_seconds`
    pub client_session_start: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_client_session_duration_seconds`
    pub client_session_duration: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_client_sessions_total`
    pub client_sessions_total: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_heartbeats_sent_total`
    pub client_heartbeats_sent: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_heartbeats_received_total`
    pub client_heartbeats_rx: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_heartbeats_missed_total`
    pub client_heartbeats_missed: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_heartbeats_consecutive_missed`
    pub client_consecutive_missed: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_client_heartbeat_rtt_seconds`
    pub client_rtt: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_client_last_heartbeat_timestamp_seconds`
    pub client_last_heartbeat: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_client_session_disconnects_total`
    pub client_disconnects: Family<DisconnectLabel, Counter>,

    /// `tcp_monitor_client_pool_sessions_active`
    pub client_pool_sessions_active: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_client_pool_sessions_planned`
    pub client_pool_sessions_planned: Family<PeerLabel, Gauge>,
    /// `tcp_monitor_client_pool_sessions_alive_by_age`
    pub client_pool_alive_by_age: Family<AgeLabel, Gauge>,
    /// `tcp_monitor_client_pool_sessions_planned_by_age`
    pub client_pool_planned_by_age: Family<AgeLabel, Gauge>,
    /// `tcp_monitor_client_pool_oldest_session_age_seconds`
    pub client_pool_oldest_age: Family<PeerLabel, FloatGauge>,
    /// `tcp_monitor_client_pool_sessions_opened_total`
    pub client_pool_opened: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_pool_session_ends_total`
    pub client_pool_ends: Family<DisconnectLabel, Counter>,
    /// `tcp_monitor_client_pool_session_age_at_end_seconds`
    pub client_pool_age_at_end: Family<DisconnectLabel, Histogram, HistogramConstructor>,
    /// `tcp_monitor_client_pool_new_session_checks_total`
    pub client_pool_checks: Family<CheckLabel, Counter>,
    /// `tcp_monitor_client_pool_heartbeats_missed_total`
    pub client_pool_heartbeats_missed: Family<PeerLabel, Counter>,
    /// `tcp_monitor_client_pool_heartbeat_rtt_seconds`
    pub client_pool_rtt: Family<PeerLabel, Histogram, HistogramConstructor>,

    /// Registry every family above is registered in; encoded by `/metrics`.
    pub registry: Registry,

    /// Inbound peer names that have allocated label entries.
    pub server_peers: ServerPeers,
}

/// The set of inbound peer names that have metric label entries, capped at
/// [`MAX_UNIQUE_SERVER_PEERS`] because prometheus-client cannot remove them.
#[derive(Debug, Default)]
pub struct ServerPeers {
    /// Names seen so far.
    seen: Mutex<HashSet<String>>,
}

impl ServerPeers {
    /// Returns true if the peer may proceed (either already known or within the cap).
    /// Returns false when the cap is reached; the caller should reject the connection.
    pub fn try_register(&self, peer: &str) -> bool {
        // The set is always left consistent, so a panic elsewhere while holding
        // the lock does not invalidate it.
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.contains(peer) {
            return true;
        }
        if seen.len() >= MAX_UNIQUE_SERVER_PEERS {
            return false;
        }
        seen.insert(peer.to_owned())
    }
}

impl Metrics {
    /// Create all metric families and register them.
    /// Returned in an `Arc` because every task shares the same instance.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "one registration per metric family; splitting would scatter the list"
    )]
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();

        macro_rules! reg {
            ($name:literal, $help:literal) => {{
                let f = Family::default();
                registry.register($name, $help, f.clone());
                f
            }};
            ($name:literal, $help:literal, $constructor:expr) => {{
                let constructor: HistogramConstructor = $constructor;
                let f = Family::new_with_constructor(constructor);
                registry.register($name, $help, f.clone());
                f
            }};
        }

        Arc::new(Self {
            server_session_active: reg!(
                "tcp_monitor_server_session_active",
                "Primary server sessions currently active with this peer (normally 0 or 1)"
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
            server_pool_sessions_active: reg!(
                "tcp_monitor_server_pool_sessions_active",
                "Pool sessions from this peer currently active"
            ),
            server_pool_ends: reg!(
                "tcp_monitor_server_pool_session_ends",
                "Pool sessions from this peer that ended, by reason (expired = planned close)"
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

            client_pool_sessions_active: reg!(
                "tcp_monitor_client_pool_sessions_active",
                "Pool sessions to this peer currently alive"
            ),
            client_pool_sessions_planned: reg!(
                "tcp_monitor_client_pool_sessions_planned",
                "Pool sessions to this peer that would be alive if none had ended early"
            ),
            client_pool_alive_by_age: reg!(
                "tcp_monitor_client_pool_sessions_alive_by_age",
                "Pool sessions alive, by age in seconds (cumulative, le = at most this old)"
            ),
            client_pool_planned_by_age: reg!(
                "tcp_monitor_client_pool_sessions_planned_by_age",
                "Pool sessions that would be alive if none had ended early, by age in seconds (cumulative)"
            ),
            client_pool_oldest_age: reg!(
                "tcp_monitor_client_pool_oldest_session_age_seconds",
                "Age of the oldest pool session alive to this peer"
            ),
            client_pool_opened: reg!(
                "tcp_monitor_client_pool_sessions_opened",
                "Pool sessions established to this peer since startup"
            ),
            client_pool_ends: reg!(
                "tcp_monitor_client_pool_session_ends",
                "Pool sessions to this peer that ended, by reason (expired = planned close)"
            ),
            client_pool_age_at_end: reg!(
                "tcp_monitor_client_pool_session_age_at_end_seconds",
                "Age of pool sessions when they ended, by reason",
                age_histogram
            ),
            client_pool_checks: reg!(
                "tcp_monitor_client_pool_new_session_checks",
                "After a pool session ends early: whether a new session to the peer works"
            ),
            client_pool_heartbeats_missed: reg!(
                "tcp_monitor_client_pool_heartbeats_missed",
                "Pool heartbeat echoes not received within the timeout window"
            ),
            client_pool_rtt: reg!(
                "tcp_monitor_client_pool_heartbeat_rtt_seconds",
                "Round-trip time of pool heartbeat echoes",
                rtt_histogram
            ),

            registry,
            server_peers: ServerPeers::default(),
        })
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

    /// Pre-create the server's pool end reasons for `peer` at zero.
    pub fn init_server_pool_labels(&self, node: &str, peer: &str) {
        for reason in SERVER_POOL_END_REASONS {
            drop(
                self.server_pool_ends
                    .get_or_create(&disconnect_label(node, peer, reason)),
            );
        }
    }

    /// Pre-create the client's pool end reasons and check outcomes for
    /// `peer` at zero, so `increase()` sees the first event.
    pub fn init_client_pool_labels(&self, node: &str, peer: &str) {
        for reason in CLIENT_POOL_END_REASONS {
            drop(
                self.client_pool_ends
                    .get_or_create(&disconnect_label(node, peer, reason)),
            );
        }
        for reason in POOL_EARLY_END_REASONS {
            for result in CHECK_RESULTS {
                drop(self.client_pool_checks.get_or_create(&CheckLabel {
                    node: node.to_owned(),
                    peer: peer.to_owned(),
                    reason: reason.to_owned(),
                    result: result.to_owned(),
                }));
            }
        }
    }

    /// Pre-create all disconnect reason label combinations so they appear in
    /// /metrics from startup rather than only after the first disconnect.
    pub fn init_disconnect_labels(&self, node: &str, peer: &str) {
        for reason in SERVER_DISCONNECT_REASONS {
            let label = DisconnectLabel {
                node: node.to_owned(),
                peer: peer.to_owned(),
                reason: reason.to_owned(),
            };
            drop(self.server_disconnects.get_or_create(&label));
        }
        for reason in CLIENT_DISCONNECT_REASONS {
            let label = DisconnectLabel {
                node: node.to_owned(),
                peer: peer.to_owned(),
                reason: reason.to_owned(),
            };
            drop(self.client_disconnects.get_or_create(&label));
        }
    }
}

/// A [`DisconnectLabel`] from borrowed parts.
#[must_use]
pub fn disconnect_label(node: &str, peer: &str, reason: &str) -> DisconnectLabel {
    DisconnectLabel {
        node: node.to_owned(),
        peer: peer.to_owned(),
        reason: reason.to_owned(),
    }
}

/// The value of the `le` label for [`POOL_AGE_BUCKETS`], then `+Inf`.
/// Written like the histogram's own `le` values ("3600.0"), so the by-age
/// gauges and the age-at-end histogram can be matched on `le`.
#[must_use]
pub fn pool_age_le_values() -> Vec<String> {
    POOL_AGE_BUCKETS
        .iter()
        .map(|bound| format!("{bound}.0"))
        .chain(std::iter::once("+Inf".to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    mod server_peers_try_register {
        use super::*;

        #[test]
        fn accepts_new_peers_up_to_the_cap() {
            let metrics = Metrics::new();

            let accepted = (0..MAX_UNIQUE_SERVER_PEERS)
                .all(|i| metrics.server_peers.try_register(&format!("p{i}")));

            assert!(accepted);
        }

        #[test]
        fn rejects_a_new_peer_once_the_cap_is_reached() {
            let metrics = Metrics::new();
            for i in 0..MAX_UNIQUE_SERVER_PEERS {
                metrics.server_peers.try_register(&format!("p{i}"));
            }

            assert!(!metrics.server_peers.try_register("one-too-many"));
        }

        #[test]
        fn still_accepts_a_known_peer_once_the_cap_is_reached() {
            let metrics = Metrics::new();
            for i in 0..MAX_UNIQUE_SERVER_PEERS {
                metrics.server_peers.try_register(&format!("p{i}"));
            }

            assert!(metrics.server_peers.try_register("p0"));
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

    mod init_pool_labels {
        use super::*;

        #[test]
        fn exposes_every_pool_end_reason_and_check_outcome_at_zero() {
            let metrics = Metrics::new();

            metrics.init_server_pool_labels("me", "them");
            metrics.init_client_pool_labels("me", "them");

            // Literal lists: the README documents these values and alerts
            // select on them.
            let text = metrics.encode().unwrap();
            let mut expected: Vec<String> = [
                "expired",
                "remote_close",
                "connection_reset",
                "timeout",
                "local_error",
            ]
            .iter()
            .map(|r| format!("tcp_monitor_server_pool_session_ends_total{{node=\"me\",peer=\"them\",reason=\"{r}\"}} 0"))
            .collect();
            for r in [
                "expired",
                "remote_close",
                "connection_reset",
                "timeout",
                "local_error",
                "connect_failed",
            ] {
                expected.push(format!(
                    "tcp_monitor_client_pool_session_ends_total{{node=\"me\",peer=\"them\",reason=\"{r}\"}} 0"
                ));
            }
            for r in ["remote_close", "connection_reset", "timeout", "local_error"] {
                for result in ["ok", "failed"] {
                    expected.push(format!(
                        "tcp_monitor_client_pool_new_session_checks_total{{node=\"me\",peer=\"them\",reason=\"{r}\",result=\"{result}\"}} 0"
                    ));
                }
            }
            for line in expected {
                assert!(text.contains(&line), "missing: {line}");
            }
        }
    }

    mod pool_age_le_values {
        use super::*;

        #[test]
        fn lists_the_documented_bounds_then_inf() {
            assert_eq!(
                pool_age_le_values(),
                [
                    "60.0", "300.0", "900.0", "1800.0", "3600.0", "7200.0", "14400.0", "28800.0",
                    "43200.0", "64800.0", "86400.0", "108000.0", "129600.0", "+Inf"
                ]
            );
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
                node: "me".to_owned(),
                peer: "them".to_owned(),
            };
            metrics.init_disconnect_labels("me", "them");
            drop(metrics.server_session_active.get_or_create(&label));
            drop(metrics.server_session_start.get_or_create(&label));
            drop(metrics.server_session_duration.get_or_create(&label));
            drop(metrics.server_sessions_total.get_or_create(&label));
            drop(metrics.server_heartbeats_rx.get_or_create(&label));
            drop(metrics.server_last_heartbeat.get_or_create(&label));
            drop(metrics.client_session_active.get_or_create(&label));
            drop(metrics.client_session_start.get_or_create(&label));
            drop(metrics.client_session_duration.get_or_create(&label));
            drop(metrics.client_sessions_total.get_or_create(&label));
            drop(metrics.client_heartbeats_sent.get_or_create(&label));
            drop(metrics.client_heartbeats_rx.get_or_create(&label));
            drop(metrics.client_heartbeats_missed.get_or_create(&label));
            drop(metrics.client_consecutive_missed.get_or_create(&label));
            drop(metrics.client_rtt.get_or_create(&label));
            drop(metrics.client_last_heartbeat.get_or_create(&label));

            metrics.init_server_pool_labels("me", "them");
            metrics.init_client_pool_labels("me", "them");
            drop(metrics.server_pool_sessions_active.get_or_create(&label));
            drop(metrics.client_pool_sessions_active.get_or_create(&label));
            drop(metrics.client_pool_sessions_planned.get_or_create(&label));
            drop(metrics.client_pool_oldest_age.get_or_create(&label));
            drop(metrics.client_pool_opened.get_or_create(&label));
            drop(metrics.client_pool_heartbeats_missed.get_or_create(&label));
            metrics.client_pool_rtt.get_or_create(&label).observe(0.001);
            metrics
                .client_pool_age_at_end
                .get_or_create(&disconnect_label("me", "them", "expired"))
                .observe(1.0);
            let age_label = AgeLabel {
                node: "me".to_owned(),
                peer: "them".to_owned(),
                le: "+Inf".to_owned(),
            };
            drop(metrics.client_pool_alive_by_age.get_or_create(&age_label));
            drop(metrics.client_pool_planned_by_age.get_or_create(&age_label));

            let text = metrics.encode().unwrap();

            for name in [
                "tcp_monitor_server_pool_sessions_active",
                "tcp_monitor_server_pool_session_ends_total",
                "tcp_monitor_client_pool_sessions_active",
                "tcp_monitor_client_pool_sessions_planned",
                "tcp_monitor_client_pool_sessions_alive_by_age",
                "tcp_monitor_client_pool_sessions_planned_by_age",
                "tcp_monitor_client_pool_oldest_session_age_seconds",
                "tcp_monitor_client_pool_sessions_opened_total",
                "tcp_monitor_client_pool_session_ends_total",
                "tcp_monitor_client_pool_session_age_at_end_seconds_sum",
                "tcp_monitor_client_pool_new_session_checks_total",
                "tcp_monitor_client_pool_heartbeats_missed_total",
                "tcp_monitor_client_pool_heartbeat_rtt_seconds_sum",
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
