//! Session pool: many staggered sessions to one peer, to catch failures that
//! one long-lived session cannot see.
//!
//! A stateful load balancer pins each TCP session to the backend it picked
//! when the session opened, and keeps it there even if that backend's path
//! dies. During maintenance only new sessions move. A single session
//! therefore tests one path, and a fresh-connection probe tests only where
//! new sessions go right now.
//!
//! The pool opens one session to the peer every `open_interval`, and closes
//! each on purpose when it reaches its planned lifetime (`expired`). At any
//! time it holds sessions pinned at many different moments, so it covers
//! every path the load balancer used over the last lifetime. A session that
//! ends before its planned lifetime was cut off; its age says when it was
//! pinned. An early end is followed by a check of whether a brand-new
//! session works (at most one check at a time per peer), which separates
//! "old pinned sessions died" from "the peer is unreachable".
//!
//! On startup the pool fills at once, with remaining lifetimes spread
//! evenly, so it reaches its full size immediately instead of after one
//! lifetime. Those first sessions all pin to wherever new sessions go at
//! startup; the spread of pin times builds up as they are replaced.

use crate::client::{heartbeat_loop, HeartbeatObserver};
use crate::config::{ClientConfig, PeerConfig, PoolConfig};
use crate::metrics::{
    disconnect_label, pool_age_le_values, AgeLabel, CheckLabel, Metrics, PeerLabel,
};
use crate::protocol::{self, SessionKind};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// How long a pool session or check may take to connect. Without it, a
/// connect into a black hole waits for the kernel's SYN retries (about two
/// minutes on Linux), which would delay the check result by as much.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Gap between sessions opened during the startup fill, so a full pool of
/// a few hundred sessions doesn't arrive at the peer as one burst.
const FILL_SPACING: Duration = Duration::from_millis(100);

/// How often the by-age gauges are recomputed. Ages grow continuously, so
/// they need a refresh between events; 15 s matches a common scrape interval.
const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// How long to try to send a goodbye before closing anyway. A goodbye that
/// cannot be sent only makes the server count the close as unplanned.
const GOODBYE_TIMEOUT: Duration = Duration::from_secs(1);

/// Everything [`run`] needs.
#[derive(Debug)]
pub struct PoolArgs {
    /// The peer the pool's sessions go to.
    pub peer: PeerConfig,
    /// Heartbeat timing, shared with the primary session.
    pub client_cfg: ClientConfig,
    /// Pool size and timing.
    pub pool_cfg: PoolConfig,
    /// This node's name, sent in the handshake and used as the `node` label.
    pub node_name: String,
    /// Shared metrics.
    pub metrics: Arc<Metrics>,
    /// Stops the pool and closes all its sessions.
    pub cancel: CancellationToken,
}

/// What every task in one pool shares.
#[derive(Debug)]
struct Ctx {
    /// The peer the pool's sessions go to.
    peer: PeerConfig,
    /// Heartbeat timing.
    client_cfg: ClientConfig,
    /// This node's name, sent in the handshake.
    node_name: String,
    /// Labels of the pool's per-peer metrics.
    label: PeerLabel,
    /// Shared metrics.
    metrics: Arc<Metrics>,
}

impl Ctx {
    /// `host:port` of the peer's heartbeat port.
    fn addr(&self) -> String {
        format!("{}:{}", self.peer.host, self.peer.heartbeat_port)
    }
}

/// What a pool session task reports back to [`run`].
#[derive(Debug, Clone, Copy)]
enum Event {
    /// The session answered its first heartbeat, so it works.
    Established {
        /// Session ID.
        id: u64,
        /// When it was established.
        opened: Instant,
        /// When it will close itself as `expired`.
        planned_end: Instant,
    },
    /// The session never worked: no connection, or no echo of its first
    /// heartbeat (for example an older peer that rejects handshake v2).
    OpenFailed {
        /// Session ID.
        id: u64,
        /// When it would have closed itself, had it worked.
        planned_end: Instant,
    },
    /// An established session ended.
    Ended {
        /// Session ID.
        id: u64,
        /// One of `CLIENT_POOL_END_REASONS`, or `"cancelled"`.
        reason: &'static str,
    },
}

/// One session the pool planned: alive, ended early, or never opened.
#[derive(Debug)]
struct Slot {
    /// When it was established, or when opening it failed.
    opened: Instant,
    /// When it would close itself as `expired`.
    planned_end: Instant,
    /// False once it has ended before `planned_end`, or if it never opened.
    /// Such a slot is kept until `planned_end`, so `planned - active`
    /// counts the sessions lost.
    alive: bool,
}

/// The pool's bookkeeping, owned by the [`run`] loop.
#[derive(Debug, Default)]
struct PoolState {
    /// Every planned session by ID.
    slots: HashMap<u64, Slot>,
    /// The new-session check in flight, if any. At most one runs at a time:
    /// when a backend fails, many pinned sessions die together, and one
    /// check answers for all of them without flooding the peer.
    check: Option<JoinHandle<()>>,
    /// Whether the last open failed, so a run of failures is logged once.
    opens_failing: bool,
}

/// Run one peer's pool until `cancel` fires, then close every session.
pub async fn run(args: PoolArgs) {
    let PoolArgs {
        peer,
        client_cfg,
        pool_cfg,
        node_name,
        metrics,
        cancel,
    } = args;
    let ctx = Arc::new(Ctx {
        label: PeerLabel {
            node: node_name.clone(),
            peer: peer.name.clone(),
        },
        peer,
        client_cfg,
        node_name,
        metrics,
    });
    ctx.metrics
        .init_client_pool_labels(&ctx.label.node, &ctx.label.peer);

    let mut tasks = JoinSet::new();
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let mut state = PoolState::default();
    // Ranges in milliseconds, bounded by config validation. Steady state:
    // lifetimes from 50% to 150% of the configured mean, open intervals ±10%.
    // Startup fill: remaining lifetimes spread from one interval to the full
    // lifetime, so closes, and therefore the steady-state opens that replace
    // them, are evenly spaced, and none closes before the first new open.
    let lifetime_range = (
        pool_cfg.lifetime.saturating_mul(500),
        pool_cfg.lifetime.saturating_mul(1_500),
    );
    let interval_range = (
        pool_cfg.open_interval.saturating_mul(900),
        pool_cfg.open_interval.saturating_mul(1_100),
    );
    let fill_range = (
        pool_cfg.open_interval.saturating_mul(1_000),
        pool_cfg.lifetime.saturating_mul(1_000),
    );

    info!(
        "Starting session pool to {}: {} sessions, a new one every {}s",
        ctx.peer.name,
        pool_cfg.target_size(),
        pool_cfg.open_interval
    );

    let mut fill_left = pool_cfg.target_size();
    let mut next_fill = Instant::now();
    let mut next_open = Instant::now() + random_millis(interval_range.0, interval_range.1);
    let mut refresh = tokio::time::interval(REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = tokio::time::sleep_until(next_fill), if fill_left > 0 => {
                tasks.spawn(pool_session(
                    Arc::clone(&ctx),
                    random_millis(fill_range.0, fill_range.1),
                    events_tx.clone(),
                    cancel.child_token(),
                ));
                fill_left -= 1;
                next_fill += FILL_SPACING;
            }
            () = tokio::time::sleep_until(next_open) => {
                tasks.spawn(pool_session(
                    Arc::clone(&ctx),
                    random_millis(lifetime_range.0, lifetime_range.1),
                    events_tx.clone(),
                    cancel.child_token(),
                ));
                next_open += random_millis(interval_range.0, interval_range.1);
            }
            Some(event) = events.recv() => {
                state.apply(&ctx, event, &cancel);
                state.refresh_gauges(&ctx);
            }
            _ = refresh.tick() => state.refresh_gauges(&ctx),
            Some(joined) = tasks.join_next() => {
                if let Err(e) = joined {
                    error!("Pool task for {} ended abnormally: {}", ctx.peer.name, e);
                }
            }
        }
    }

    // Every session and the check hold a child token of `cancel`, so they
    // are all stopping now; wait for their goodbyes.
    while let Some(joined) = tasks.join_next().await {
        if let Err(e) = joined {
            error!("Pool task for {} ended abnormally: {}", ctx.peer.name, e);
        }
    }
    if let Some(check) = state.check.take() {
        if let Err(e) = check.await {
            error!("Pool check for {} ended abnormally: {}", ctx.peer.name, e);
        }
    }
    state.slots.clear();
    state.refresh_gauges(&ctx);
}

impl PoolState {
    /// Apply one session event to the slots and counters, and start a
    /// new-session check after an early end.
    fn apply(&mut self, ctx: &Arc<Ctx>, event: Event, cancel: &CancellationToken) {
        let metrics = &ctx.metrics;
        let (node, peer) = (&ctx.label.node, &ctx.label.peer);
        match event {
            Event::Established {
                id,
                opened,
                planned_end,
            } => {
                metrics.client_pool_opened.get_or_create(&ctx.label).inc();
                if self.opens_failing {
                    info!("Pool sessions to {} open again", peer);
                    self.opens_failing = false;
                }
                self.slots.insert(
                    id,
                    Slot {
                        opened,
                        planned_end,
                        alive: true,
                    },
                );
            }
            Event::OpenFailed { id, planned_end } => {
                metrics
                    .client_pool_ends
                    .get_or_create(&disconnect_label(node, peer, "connect_failed"))
                    .inc();
                if !self.opens_failing {
                    warn!("Pool sessions to {} fail to open", peer);
                    self.opens_failing = true;
                }
                self.slots.insert(
                    id,
                    Slot {
                        opened: Instant::now(),
                        planned_end,
                        alive: false,
                    },
                );
            }
            Event::Ended {
                reason: "cancelled",
                id,
            } => {
                self.slots.remove(&id);
            }
            Event::Ended { id, reason } => {
                metrics
                    .client_pool_ends
                    .get_or_create(&disconnect_label(node, peer, reason))
                    .inc();
                let Some(slot) = self.slots.get_mut(&id) else {
                    // Only established sessions send `Ended`, and their slot
                    // stays until they end; nothing is left to update.
                    return;
                };
                let age = slot.opened.elapsed();
                metrics
                    .client_pool_age_at_end
                    .get_or_create(&disconnect_label(node, peer, reason))
                    .observe(age.as_secs_f64());
                if reason == "expired" {
                    self.slots.remove(&id);
                    debug!("Pool session expired: peer={} session_id={:016x}", peer, id);
                    return;
                }
                slot.alive = false;
                warn!(
                    "Pool session ended early: peer={} session_id={:016x} age={}s reason={}",
                    peer,
                    id,
                    age.as_secs(),
                    reason
                );
                self.start_check(ctx, reason, cancel);
            }
        }
    }

    /// Start a new-session check, unless one is already running.
    fn start_check(&mut self, ctx: &Arc<Ctx>, reason: &'static str, cancel: &CancellationToken) {
        if self
            .check
            .as_ref()
            .is_some_and(|check| !check.is_finished())
        {
            debug!("New-session check to {} already running", ctx.peer.name);
            return;
        }
        self.check = Some(tokio::spawn(new_session_check(
            Arc::clone(ctx),
            reason,
            cancel.child_token(),
        )));
    }

    /// Recompute the pool gauges from the slots, first dropping ended slots
    /// whose planned end has passed.
    fn refresh_gauges(&mut self, ctx: &Ctx) {
        let now = Instant::now();
        self.slots
            .retain(|_, slot| slot.alive || slot.planned_end > now);

        let metrics = &ctx.metrics;
        let label = &ctx.label;
        let ages: Vec<(u64, bool)> = self
            .slots
            .values()
            .map(|slot| (now.duration_since(slot.opened).as_secs(), slot.alive))
            .collect();
        let alive = ages.iter().filter(|(_, alive)| *alive);

        metrics
            .client_pool_sessions_active
            .get_or_create(label)
            .set(count(alive.clone()));
        metrics
            .client_pool_sessions_planned
            .get_or_create(label)
            .set(count(ages.iter()));
        metrics.client_pool_oldest_age.get_or_create(label).set(
            alive.clone().map(|(age, _)| *age).max().map_or(0.0, |age| {
                // Whole seconds are plenty for ages of minutes to days.
                f64::from(u32::try_from(age).unwrap_or(u32::MAX))
            }),
        );

        let bounds = crate::metrics::POOL_AGE_BUCKETS
            .iter()
            .map(|b| u64::from(*b))
            .map(Some)
            .chain(std::iter::once(None));
        for (bound, le) in bounds.zip(pool_age_le_values()) {
            let within = |(age, _): &&(u64, bool)| bound.is_none_or(|b| *age <= b);
            let age_label = AgeLabel {
                node: label.node.clone(),
                peer: label.peer.clone(),
                le,
            };
            metrics
                .client_pool_alive_by_age
                .get_or_create(&age_label)
                .set(count(alive.clone().filter(within)));
            metrics
                .client_pool_planned_by_age
                .get_or_create(&age_label)
                .set(count(ages.iter().filter(within)));
        }
    }
}

/// Number of items, as a gauge value.
fn count<T>(items: impl Iterator<Item = T>) -> i64 {
    i64::try_from(items.count()).unwrap_or(i64::MAX)
}

/// One pool session, from connect to close. Reports [`Event`]s to the pool.
///
/// The session counts as established only once its first heartbeat is
/// echoed. A connection the peer accepts but then rejects (an older peer
/// without handshake v2, or one at its connection limit) is therefore an
/// `OpenFailed`, not an early end, and triggers no new-session check.
async fn pool_session(
    ctx: Arc<Ctx>,
    lifetime: Duration,
    events: mpsc::UnboundedSender<Event>,
    cancel: CancellationToken,
) {
    let id = random_u64();
    let report = |event| {
        // The pool keeps the receiver until every session has finished, so
        // this cannot fail unless the pool task itself has died.
        if events.send(event).is_err() {
            debug!("Pool for {} is gone; event dropped", ctx.peer.name);
        }
    };
    let open_failed = || Event::OpenFailed {
        id,
        planned_end: Instant::now() + lifetime,
    };

    let echo_timeout = Duration::from_secs(ctx.client_cfg.heartbeat_interval);
    let open_result = tokio::select! {
        r = tokio::time::timeout(CONNECT_TIMEOUT + echo_timeout, open_session(&ctx, id)) => r,
        () = cancel.cancelled() => return,
    };
    let mut stream = match open_result {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            debug!("Pool session to {} failed to open: {}", ctx.addr(), e);
            report(open_failed());
            return;
        }
        Err(_elapsed) => {
            debug!("Pool session to {} timed out opening", ctx.addr());
            report(open_failed());
            return;
        }
    };

    let opened = Instant::now();
    let planned_end = opened + lifetime;
    report(Event::Established {
        id,
        opened,
        planned_end,
    });

    let reason = heartbeat_loop(
        &mut stream,
        &ctx.client_cfg,
        Some(planned_end),
        &cancel,
        &PoolObserver { ctx: &ctx },
    )
    .await;
    if matches!(reason, "expired" | "cancelled") {
        say_goodbye(&mut stream).await;
    }
    report(Event::Ended { id, reason });
}

/// Connect, send the pool handshake, and get the first heartbeat echoed.
async fn open_session(ctx: &Ctx, id: u64) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(ctx.addr()).await?;
    protocol::send_handshake_v2(&mut stream, &ctx.node_name, SessionKind::Pool, id).await?;
    protocol::send_heartbeat(&mut stream, 0).await?;
    let echo = protocol::recv_heartbeat(&mut stream).await?;
    if echo.seq == 0 {
        Ok(stream)
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "wrong echo"))
    }
}

/// Records pool heartbeats in the `tcp_monitor_client_pool_*` metrics.
/// Misses are not logged per session: a pool has hundreds.
#[derive(Debug)]
struct PoolObserver<'a> {
    /// The pool the session belongs to.
    ctx: &'a Ctx,
}

impl HeartbeatObserver for PoolObserver<'_> {
    fn sent(&self) {
        self.ctx
            .metrics
            .client_pool_heartbeats_sent
            .get_or_create(&self.ctx.label)
            .inc();
    }

    fn echo(&self, rtt: Option<f64>) {
        self.ctx
            .metrics
            .client_pool_heartbeats_rx
            .get_or_create(&self.ctx.label)
            .inc();
        if let Some(rtt) = rtt {
            self.ctx
                .metrics
                .client_pool_rtt
                .get_or_create(&self.ctx.label)
                .observe(rtt);
        }
    }

    fn missed(&self, _consecutive: i64) {
        self.ctx
            .metrics
            .client_pool_heartbeats_missed
            .get_or_create(&self.ctx.label)
            .inc();
    }
}

/// Tell the server this close is planned, then close our side. Best effort.
async fn say_goodbye(stream: &mut TcpStream) {
    let goodbye = async {
        protocol::send_goodbye(stream).await?;
        stream.shutdown().await
    };
    match tokio::time::timeout(GOODBYE_TIMEOUT, goodbye).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => debug!("Pool goodbye failed: {}", e),
        Err(_elapsed) => debug!("Pool goodbye timed out"),
    }
}

/// After a pool session ended early with `reason`: does a brand-new
/// session to the peer work? Records the answer in
/// `tcp_monitor_client_pool_new_session_checks_total`.
async fn new_session_check(ctx: Arc<Ctx>, reason: &'static str, cancel: CancellationToken) {
    let echo_timeout = Duration::from_secs(ctx.client_cfg.heartbeat_interval);
    let result = tokio::select! {
        r = tokio::time::timeout(CONNECT_TIMEOUT + echo_timeout, check_once(&ctx)) => r,
        () = cancel.cancelled() => return,
    };
    let ok = matches!(result, Ok(Ok(())));
    if ok {
        info!(
            "New session to {} works after a pool session ended early ({})",
            ctx.peer.name, reason
        );
    } else {
        warn!(
            "New session to {} also fails after a pool session ended early ({})",
            ctx.peer.name, reason
        );
    }
    ctx.metrics
        .client_pool_checks
        .get_or_create(&CheckLabel {
            node: ctx.label.node.clone(),
            peer: ctx.label.peer.clone(),
            reason: reason.to_owned(),
            result: if ok { "ok" } else { "failed" }.to_owned(),
        })
        .inc();
}

/// Connect, handshake as a check, and get one heartbeat echoed.
async fn check_once(ctx: &Ctx) -> io::Result<()> {
    let mut stream = TcpStream::connect(ctx.addr()).await?;
    protocol::send_handshake_v2(
        &mut stream,
        &ctx.node_name,
        SessionKind::Check,
        random_u64(),
    )
    .await?;
    protocol::send_heartbeat(&mut stream, 0).await?;
    let echo = protocol::recv_heartbeat(&mut stream).await?;
    say_goodbye(&mut stream).await;
    if echo.seq == 0 {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "wrong echo"))
    }
}

/// A random duration from `lo_ms` to `hi_ms` milliseconds, inclusive.
fn random_millis(lo_ms: u64, hi_ms: u64) -> Duration {
    let span = hi_ms.saturating_sub(lo_ms).saturating_add(1);
    Duration::from_millis(lo_ms.saturating_add(random_u64() % span))
}

/// A random number for jitter and session IDs; not for anything secret.
///
/// Uses the standard library's randomly keyed `SipHash` on a counter, which
/// avoids a dependency for numbers that only need to differ.
fn random_u64() -> u64 {
    /// Makes every call hash a different input.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    mod pool_state_refresh_gauges {
        use super::*;

        fn ctx() -> Ctx {
            Ctx {
                peer: PeerConfig {
                    name: "srv".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    heartbeat_port: 1,
                },
                client_cfg: ClientConfig {
                    heartbeat_interval: 30,
                    max_misses: 3,
                    reconnect_delay: 10,
                    pool: None,
                },
                node_name: "cli".to_owned(),
                label: PeerLabel {
                    node: "cli".to_owned(),
                    peer: "srv".to_owned(),
                },
                metrics: Metrics::new(),
            }
        }

        /// A slot opened `age` seconds ago that would end `left` seconds from
        /// now (negative: already past).
        fn slot(age: u64, left: i64, alive: bool) -> Slot {
            let now = Instant::now();
            let opened = now.checked_sub(Duration::from_secs(age)).unwrap();
            let planned_end = if left >= 0 {
                now + Duration::from_secs(left.unsigned_abs())
            } else {
                now.checked_sub(Duration::from_secs(left.unsigned_abs()))
                    .unwrap()
            };
            Slot {
                opened,
                planned_end,
                alive,
            }
        }

        /// Value of `name` for `le`, or the plain sample when `le` is empty.
        fn gauge(ctx: &Ctx, name: &str, le: &str) -> String {
            let text = ctx.metrics.encode().unwrap();
            let prefix = if le.is_empty() {
                format!("{name}{{node=\"cli\",peer=\"srv\"}} ")
            } else {
                format!("{name}{{node=\"cli\",peer=\"srv\",le=\"{le}\"}} ")
            };
            text.lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .to_owned()
        }

        fn refreshed() -> Ctx {
            let ctx = ctx();
            let mut state = PoolState {
                slots: HashMap::from([
                    // Alive for 4000 s.
                    (1, slot(4_000, 100, true)),
                    // Ended early 100 s in; would still be alive.
                    (2, slot(100, 100, false)),
                    // Ended early, and its planned end has passed: forgotten.
                    (3, slot(5_000, -1, false)),
                ]),
                ..PoolState::default()
            };
            state.refresh_gauges(&ctx);
            ctx
        }

        #[test]
        fn counts_only_live_sessions_as_active() {
            let ctx = refreshed();

            assert_eq!(
                gauge(&ctx, "tcp_monitor_client_pool_sessions_active", ""),
                "1"
            );
        }

        #[test]
        fn counts_early_ends_as_planned_until_their_planned_end() {
            let ctx = refreshed();

            assert_eq!(
                gauge(&ctx, "tcp_monitor_client_pool_sessions_planned", ""),
                "2"
            );
        }

        #[test]
        fn reports_the_age_of_the_oldest_live_session() {
            let ctx = refreshed();

            assert_eq!(
                gauge(
                    &ctx,
                    "tcp_monitor_client_pool_oldest_session_age_seconds",
                    ""
                ),
                "4000.0"
            );
        }

        #[test]
        fn counts_live_sessions_cumulatively_by_age() {
            let ctx = refreshed();
            let name = "tcp_monitor_client_pool_sessions_alive_by_age";

            assert_eq!(
                [
                    gauge(&ctx, name, "3600.0"),
                    gauge(&ctx, name, "7200.0"),
                    gauge(&ctx, name, "+Inf")
                ],
                ["0", "1", "1"]
            );
        }

        #[test]
        fn counts_planned_sessions_cumulatively_by_age() {
            let ctx = refreshed();
            let name = "tcp_monitor_client_pool_sessions_planned_by_age";

            assert_eq!(
                [
                    gauge(&ctx, name, "60.0"),
                    gauge(&ctx, name, "300.0"),
                    gauge(&ctx, name, "+Inf")
                ],
                ["0", "1", "2"]
            );
        }
    }

    mod random_millis {
        use super::*;

        #[test]
        fn stays_within_the_bounds() {
            let all_within = (0..1_000)
                .map(|_| random_millis(500, 1_500))
                .all(|d| (500..=1_500).contains(&d.as_millis()));

            assert!(all_within);
        }

        #[test]
        fn returns_the_bound_when_both_are_equal() {
            assert_eq!(random_millis(7, 7), Duration::from_millis(7));
        }

        #[test]
        fn varies_between_calls() {
            let first = random_millis(0, u64::MAX - 1);

            assert!((0..10).any(|_| random_millis(0, u64::MAX - 1) != first));
        }
    }
}
