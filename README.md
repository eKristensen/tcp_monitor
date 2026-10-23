# TCP Session Longevity Monitor

Monitors long-lived TCP sessions between nodes in a network.  
Each node runs a single binary that acts as both server and client — a mesh
of any shape is possible by editing the config file without restarting the service.

Exposes Prometheus metrics on each node covering session state, duration,
heartbeat health, disconnect reasons, and round-trip time.

---

## How it works

```
Node A                              Node B
+------------------------------+    +------------------------------+
| tcp-monitor                  |    | tcp-monitor                  |
|                              |    |                              |
|  heartbeat port :9700 <------+----+-- client session             |
|  client session  --------+   |    |   heartbeat port :9700       |
|  probe port     :9701    +---+--->+--> (same binary, both roles) |
|  metrics        :9702        |    |  probe port     :9701        |
|      ^                       |    |  metrics        :9702        |
|   Blackbox / Prometheus      |    |      ^                       |
+------------------------------+    |   Prometheus / Blackbox      |
                                    +------------------------------+
```

- Every node **listens** on the heartbeat port. New clients can connect without
  any server-side config change or restart.
- Nodes that have peers configured **connect** as clients, sending a heartbeat
  every 30 s (configurable) and measuring echo round-trip time.
- On connect the client sends its **node name**; the server uses this to label
  per-session metrics, so Prometheus can identify each session by name rather
  than IP.
- Adding or removing `[[peers]]` from the config file is picked up live via
  `systemctl reload tcp-monitor` (SIGHUP) — no restart, no disruption to
  existing sessions.
- The **probe port** accepts connections, sends `TCP-MONITOR OK\r\n`, and holds
  the connection open. Use it with the Prometheus Blackbox Exporter for both
  TCP establishment and banner-response tests.
- Optionally, a **session pool** per peer keeps many sessions of different ages
  open, to test paths through stateful load balancers that pin each session to
  one backend. See [Session pools](#session-pools-stateful-load-balancers).

---

## Ports (all configurable)

| Port | Purpose |
|------|---------|
| 9700 | Heartbeat port — peer clients connect here |
| 9701 | Blackbox Exporter probe port |
| 9702 | Prometheus `/metrics` scrape endpoint |

---

## Metrics reference

All metrics carry `node` (this host's name) and `peer` labels.

### Server-side (inbound sessions)

| Metric | Type | Description |
|--------|------|-------------|
| `tcp_monitor_server_session_active` | Gauge | Primary sessions live with this peer (normally 0 or 1; briefly 2 while a reconnecting client's old session times out) |
| `tcp_monitor_server_session_start_timestamp_seconds` | Gauge | When the current/last session started |
| `tcp_monitor_server_session_duration_seconds` | Gauge | Duration of active or last session |
| `tcp_monitor_server_sessions_total` | Counter | Total inbound primary sessions since startup (pool sessions are counted separately) |
| `tcp_monitor_server_heartbeats_received_total` | Counter | Heartbeat packets received on primary sessions from peer |
| `tcp_monitor_server_last_heartbeat_timestamp_seconds` | Gauge | Time of most recent heartbeat |
| `tcp_monitor_server_session_disconnects_total{reason}` | Counter | Disconnects by reason |

### Client-side (outbound sessions)

| Metric | Type | Description |
|--------|------|-------------|
| `tcp_monitor_client_session_active` | Gauge | 1 while a session is live to this peer |
| `tcp_monitor_client_session_start_timestamp_seconds` | Gauge | When the current/last session started |
| `tcp_monitor_client_session_duration_seconds` | Gauge | Duration of active or last session |
| `tcp_monitor_client_sessions_total` | Counter | Total outbound sessions since startup |
| `tcp_monitor_client_heartbeats_sent_total` | Counter | Heartbeat packets sent |
| `tcp_monitor_client_heartbeats_received_total` | Counter | Echoes received from peer |
| `tcp_monitor_client_heartbeats_missed_total` | Counter | Echoes not received within timeout |
| `tcp_monitor_client_heartbeats_consecutive_missed` | Gauge | Current run of consecutive misses |
| `tcp_monitor_client_heartbeat_rtt_seconds` | Gauge | RTT of most recent echo |
| `tcp_monitor_client_last_heartbeat_timestamp_seconds` | Gauge | Time of most recent successful echo |
| `tcp_monitor_client_session_disconnects_total{reason}` | Counter | Disconnects by reason |

### Session pools

Only present when `[client.pool]` is configured (client side), or when a peer
with a pool connects (server side).

| Metric | Type | Description |
|--------|------|-------------|
| `tcp_monitor_client_pool_sessions_active` | Gauge | Pool sessions alive |
| `tcp_monitor_client_pool_sessions_planned` | Gauge | Pool sessions that would be alive if every one had opened and none had ended early. `planned - active` = sessions lost |
| `tcp_monitor_client_pool_sessions_alive_by_age{le}` | Gauge | Alive sessions at most `le` seconds old (cumulative, like histogram buckets) |
| `tcp_monitor_client_pool_sessions_planned_by_age{le}` | Gauge | The same for planned sessions; divide to get survival by age |
| `tcp_monitor_client_pool_oldest_session_age_seconds` | Gauge | Age of the oldest live session |
| `tcp_monitor_client_pool_sessions_opened_total` | Counter | Pool sessions established (first heartbeat answered) |
| `tcp_monitor_client_pool_session_ends_total{reason}` | Counter | Pool sessions ended, by reason; `expired` is the planned close |
| `tcp_monitor_client_pool_session_age_at_end_seconds{reason}` | Histogram | Session age when it ended, by reason |
| `tcp_monitor_client_pool_new_session_checks_total{reason,result}` | Counter | After an early end: did a brand-new session work (`ok`/`failed`)? At most one check runs at a time per peer |
| `tcp_monitor_client_pool_heartbeats_sent_total` | Counter | Heartbeats sent on pool sessions |
| `tcp_monitor_client_pool_heartbeats_received_total` | Counter | Echoes received on pool sessions |
| `tcp_monitor_client_pool_heartbeats_missed_total` | Counter | Pool heartbeat echoes not received in time |
| `tcp_monitor_client_pool_heartbeat_rtt_seconds` | Histogram | Pool heartbeat round-trip time |
| `tcp_monitor_server_pool_sessions_active` | Gauge | Pool sessions from this peer currently open |
| `tcp_monitor_server_pool_session_ends_total{reason}` | Counter | Pool sessions from this peer ended, by reason; `expired` is the planned close |

### Disconnect reasons

| Reason | What it means |
|--------|---------------|
| `remote_close` | Remote sent FIN — deliberate graceful shutdown |
| `connection_reset` | RST received — remote crash, firewall rule, or network device |
| `timeout` | No data for N intervals, no FIN or RST — likely **silent network drop** (NAT expiry, middlebox) |
| `local_error` | OS error on this host — interface down, routing failure |
| `connect_failed` | Could not complete TCP handshake (client only). For pool sessions: the session never worked (no connection, or its first heartbeat got no echo) |
| `expired` | Pool sessions only: closed on purpose at the end of the planned lifetime, or because the client stopped |

**Distinguishing network from host:**
- `timeout` → suspect the **network**
- `remote_close` / `connection_reset` → suspect a **host**
- `local_error` → suspect **this host**

---

## Session pools (stateful load balancers)

### The problem

A stateful network load balancer (one shared address in front, several backends
behind it) pins every TCP session to the backend it picked when the session
opened. The session stays on that backend for its whole life, even if the
backend or its path dies. During maintenance, the load balancer sends *new*
sessions to the remaining backend while existing sessions stay where they are.

That makes ordinary monitoring blind to the failure that matters:

- One long-lived session tests only the backend it was pinned to.
- A fresh-connection probe tests only where new sessions go right now.
- Opening many sessions at the same moment can land them all on one backend,
  for example during a drain.

### How the pool works

With `[client.pool]` set, each peer gets a pool of sessions next to its primary
session:

- One new session is opened every `open_interval` (±10%), so the pool holds
  sessions opened at many different moments and therefore pinned to whichever
  backends were taking new sessions at those times. The monitor never needs to
  know which backend a session is on; it works the same for any vendor, with
  or without NAT.
- Each session draws a planned lifetime of 50%–150% of `lifetime` and closes
  itself when it gets there. It sends a goodbye first, so both ends record the
  close as `expired`.
- A session counts as opened once its first heartbeat is echoed. A session
  that never gets that far ends as `connect_failed`.
- Every session heartbeats like the primary session (`[client]`
  `heartbeat_interval` and `max_misses`). A session that ends before its
  planned lifetime **ended early**: that is the signal. A session that is
  missing echoes when its lifetime is up is not closed as `expired`; it keeps
  going until it recovers or times out, so a failing path is never hidden as a
  planned close.
- After an early end the client opens one short check session and records
  whether it works. `result="ok"` means *old pinned sessions died but new
  sessions work*: a dead backend or path behind the load balancer.
  `result="failed"` means the peer is unreachable altogether. Only one check
  runs at a time per peer, so a backend failure that kills many sessions at
  once does not flood the peer with checks.
- On startup the pool fills at once (one session every 100 ms), with remaining
  lifetimes spread from one `open_interval` to one `lifetime`, so it is at full
  size immediately. Those first sessions all pin to wherever new sessions go
  at startup; the spread of pin times builds up as they are replaced over one
  lifetime.

```toml
[client.pool]
open_interval = 900     # a new session to each peer every 15 minutes
lifetime      = 86400   # each lives about a day: 96 sessions per peer
```

Sizing: `lifetime / open_interval` sessions per peer (at most 1000). Each is one
inbound connection on the peer, which accepts at most 2048 in total from all
clients. Shorter intervals sample the load balancer's decisions more often;
longer lifetimes keep older pinned sessions under test.

**Upgrade order:** pool sessions use version 2 of the handshake. Upgrade every
peer before enabling `[client.pool]` on any node that connects to it. Older
servers reject the version 2 handshake, so every pool session to them ends as
`connect_failed`, the pool stays empty, and `TcpMonitorPoolDepleted` fires.
Primary sessions keep the version 1 handshake and work in mixed versions.

### Reading the data

```promql
# Sessions lost (would still be alive) per path
tcp_monitor_client_pool_sessions_planned - tcp_monitor_client_pool_sessions_active

# Survival by age: the fraction of sessions of each age that are still alive.
# Draw as a heatmap or a bar per le. A dead backend shows as a drop at the
# ages that were pinned to it.
tcp_monitor_client_pool_sessions_alive_by_age
  / tcp_monitor_client_pool_sessions_planned_by_age

# Early ends in the last hour, by path and reason
sum by (node, peer, reason) (
  increase(tcp_monitor_client_pool_session_ends_total{reason!="expired"}[1h]))

# When the sessions that died early had been opened (age at death)
sum by (le) (increase(
  tcp_monitor_client_pool_session_age_at_end_seconds_bucket{reason="timeout"}[1h]))

# Old sessions died, new sessions work: the pinned-path failure
increase(tcp_monitor_client_pool_new_session_checks_total{result="ok"}[1h])
```

The age of a session that died early tells you when it was pinned. While new
sessions are spread over all backends, a failed backend takes sessions of every
age. While the load balancer sends all new sessions to one backend (during a
drain), the ages show which backend took them.

### Locating the failure

Give each node 3–5 peers whose paths cross the load balancer in different
ways. Then where the early ends appear tells you where the failure is:

| Early ends seen on | Most likely cause |
|--------------------|-------------------|
| One node → one peer | That path only |
| One node → all of its peers | Near that node: its uplink, or a load balancer all its paths share |
| All nodes → one peer | Near that peer |
| Only some sessions on a path, and the check says `ok` | A backend that some sessions are pinned to |

The server side counts the same sessions (`tcp_monitor_server_pool_session_ends_total`),
so you can also see whether each end showed up on both sides (a cut path) or
on one side only.

### Alerts

[`prometheus_alerts.example.yml`](prometheus_alerts.example.yml) has example
rules: pinned sessions died while new ones work, peer unreachable, early ends to
several peers from one node, and a pool depleted below 90% of planned (which
also covers a peer that pool sessions cannot open to at all). The path alerts
include every early-end reason: a load balancer may cut a pinned session with
a FIN (`remote_close`) as well as an RST or silence, and the new-session check
is what separates "pinned path died" from "peer down". A restart of the peer's
tcp-monitor also ends its pool sessions early; expect one alert per restart.

---

## Setup

### 1. Build

```bash
cargo build --release
sudo cp target/release/tcp-monitor /usr/local/bin/tcp-monitor
```

### 2. Create config

```bash
sudo mkdir -p /etc/tcp-monitor
sudo cp config.example.toml /etc/tcp-monitor/config.toml
sudo $EDITOR /etc/tcp-monitor/config.toml
```

Minimal config (server-only node, no outbound sessions):

```toml
[node]
name = "server1"

[server]
bind                   = "0.0.0.0"
heartbeat_port         = 9700
probe_port             = 9701
metrics_port           = 9702
heartbeat_recv_timeout = 90
probe_idle_timeout     = 30
```

To also connect as a client, add:

```toml
[client]
heartbeat_interval = 30
max_misses         = 3
reconnect_delay    = 10

[[peers]]
name = "server2"
host = "192.168.1.2"
```

### 3. Open firewall ports

```bash
# On every node:
sudo firewall-cmd --permanent --add-port=9700/tcp   # heartbeat
sudo firewall-cmd --permanent --add-port=9701/tcp   # probe
sudo firewall-cmd --permanent --add-port=9702/tcp   # metrics
sudo firewall-cmd --reload
```

### 4. Install and start the service

```bash
sudo cp tcp-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tcp-monitor
sudo systemctl status tcp-monitor
```

### 5. Configure Prometheus

```yaml
scrape_configs:
  - job_name: tcp_monitor
    static_configs:
      - targets: ['localhost:9702']
```

Because metrics already carry `node` and `peer` labels, no extra relabelling
is needed to distinguish sessions.

### 6. Configure Blackbox Exporter (optional)

Copy `blackbox.example.yml` into your Blackbox Exporter config (or merge the
module into an existing config file):

```yaml
modules:
  tcp_monitor_probe:
    prober: tcp
    timeout: 5s
    tcp:
      query_response:
        - expect: "TCP-MONITOR OK"
```

This checks both that the TCP connection succeeds and that the binary responds
with the correct banner — confirming the process is running and reachable.

Prometheus scrape:

```yaml
- job_name: blackbox_tcp_monitor
  metrics_path: /probe
  params:
    module: [tcp_monitor_banner]
  static_configs:
    - targets:
        - server2.example.com:9701
  relabel_configs:
    - source_labels: [__address__]
      target_label: __param_target
    - source_labels: [__param_target]
      target_label: instance
    - target_label: __address__
      replacement: localhost:9115   # blackbox exporter address
```

### 7. Verify

```bash
# Metrics endpoint
curl http://localhost:9702/metrics | grep tcp_monitor

# Probe port
nc -w2 localhost 9701   # should print "TCP-MONITOR OK"

# Logs
journalctl -u tcp-monitor -f
```

---

## Adding a new peer (hot-reload)

Edit `/etc/tcp-monitor/config.toml` and add a `[[peers]]` block, then run:

```bash
sudo systemctl reload tcp-monitor
```

The service picks up the new peer immediately with no restart. Existing
sessions are unaffected. If the config has a syntax error the reload is
rejected and the running config continues — the error is logged to journald.

---

## Log level

Set the `LOG_LEVEL` environment variable (e.g. `LOG_LEVEL=debug`) or add it to
the service file:

```ini
[Service]
Environment=LOG_LEVEL=debug
```

---

## Useful Prometheus queries

```promql
# Is the session up right now?
tcp_monitor_client_session_active

# RTT over time
tcp_monitor_client_heartbeat_rtt_seconds

# Rate of missed heartbeats (should be 0 on a healthy session)
rate(tcp_monitor_client_heartbeats_missed_total[5m])

# Disconnect events broken down by reason
increase(tcp_monitor_client_session_disconnects_total[1h])

# How long has the current session been running?
tcp_monitor_client_session_duration_seconds
  * on(node, peer) tcp_monitor_client_session_active
```

---

## Notes

- **Heartbeats prevent NAT/firewall expiry.** The 30 s heartbeat keeps sessions
  alive through most default NAT idle timers (typically 5–30 min).
- **TCP keepalives** are also enabled at the OS level (idle 60 s, probe every
  10 s, 6 probes) as a belt-and-suspenders measure for detecting dead connections
  faster than `max_misses × heartbeat_interval` would.
- **The reconnect loop** means you accumulate a history of sessions over time.
  A pattern of `timeout` reasons strongly points to the network; `connection_reset`
  or `remote_close` points to a host.

---

## Development

```bash
cargo fmt --check                                   # formatting
cargo clippy --all-targets --locked -- -D warnings  # strict lint policy (see Cargo.toml [lints])
cargo test --locked                                 # unit + loopback integration tests
```

The integration tests in `tests/` run a real server and client on loopback, using ports
the OS assigns, so they never conflict with a running instance. The scripts in `ci/` test
the release binary end to end. Minimum supported Rust is 1.85 (`rust-version` in `Cargo.toml`).

Planned work and ideas still being considered are in [TODO.md](TODO.md).
