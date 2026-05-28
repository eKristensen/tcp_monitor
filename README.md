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
| `tcp_monitor_server_session_active` | Gauge | 1 while a session is live with this peer |
| `tcp_monitor_server_session_start_timestamp_seconds` | Gauge | When the current/last session started |
| `tcp_monitor_server_session_duration_seconds` | Gauge | Duration of active or last session |
| `tcp_monitor_server_sessions_total` | Counter | Total inbound sessions since startup |
| `tcp_monitor_server_heartbeats_received_total` | Counter | Heartbeat packets received from peer |
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

### Disconnect reasons

| Reason | What it means |
|--------|---------------|
| `remote_close` | Remote sent FIN — deliberate graceful shutdown |
| `connection_reset` | RST received — remote crash, firewall rule, or network device |
| `timeout` | No data for N intervals, no FIN or RST — likely **silent network drop** (NAT expiry, middlebox) |
| `local_error` | OS error on this host — interface down, routing failure |
| `connect_failed` | Could not complete TCP handshake (client only) |

**Distinguishing network from host:**
- `timeout` → suspect the **network**
- `remote_close` / `connection_reset` → suspect a **host**
- `local_error` → suspect **this host**

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
port                   = 9700
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
sudo firewall-cmd --permanent --add-port=9702/tcp   # metrics (local only)
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
