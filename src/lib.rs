//! TCP session longevity monitor.
//!
//! Every node runs the same binary: a server that echoes heartbeats from peer
//! clients, a client that keeps sessions open to configured peers, a probe
//! port for the Blackbox Exporter, and a Prometheus metrics endpoint.
//!
//! The crate is split into a library and a thin `main.rs` so the integration
//! tests in `tests/` can drive the server and client directly.

pub mod client;
pub mod config;
pub mod metrics;
pub mod protocol;
pub mod server;

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as seconds since the Unix epoch.
/// Returns 0.0 if the system clock is set before 1970.
#[must_use]
pub fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
