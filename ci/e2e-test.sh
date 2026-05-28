#!/usr/bin/env bash
# End-to-end heartbeat test.
# Starts a server and a client; after a few 2-second heartbeats both sides
# must show non-zero counters and a non-zero RTT in /metrics.
# Run from the repository root: ci/e2e-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

cat > /tmp/e2e-server.toml << 'TOML'
[node]
name = "ci-server"
[server]
bind         = "0.0.0.0"
port         = 19710
metrics_port = 19711
probe_port   = 19712
recv_timeout = 10
TOML

cat > /tmp/e2e-client.toml << 'TOML'
[node]
name = "ci-client"
[server]
bind         = "0.0.0.0"
port         = 19720
metrics_port = 19721
probe_port   = 19722
recv_timeout = 10
[client]
heartbeat_interval = 2
max_misses         = 3
reconnect_delay    = 2
[[peers]]
name = "ci-server"
host = "127.0.0.1"
port = 19710
TOML

"$BIN" --config /tmp/e2e-server.toml &
SRV=$!
"$BIN" --config /tmp/e2e-client.toml &
CLI=$!
trap 'kill "$SRV" "$CLI" 2>/dev/null || true; wait "$SRV" "$CLI" 2>/dev/null || true' EXIT

curl -sf --retry 15 --retry-delay 1 --retry-connrefused http://localhost:19711/metrics > /dev/null
curl -sf --retry 15 --retry-delay 1 --retry-connrefused http://localhost:19721/metrics > /dev/null

sleep 8  # allow at least 3 heartbeats at 2 s interval

SRV_M=$(curl -sf http://localhost:19711/metrics)
CLI_M=$(curl -sf http://localhost:19721/metrics)

SRV_HB=$(echo "$SRV_M" | grep 'tcp_monitor_server_heartbeats_received_total{node="ci-server",peer="ci-client"}' | awk '{print $2}')
CLI_HB=$(echo "$CLI_M" | grep 'tcp_monitor_client_heartbeats_received_total{node="ci-client",peer="ci-server"}' | awk '{print $2}')
RTT=$(echo "$CLI_M"   | grep 'tcp_monitor_client_heartbeat_rtt_seconds{'                                        | awk '{print $2}')

[[ -n "$SRV_HB" ]] && [[ "${SRV_HB%.*}" -ge 2 ]] || { echo "ERROR: server heartbeats=$SRV_HB"; echo "$SRV_M"; exit 1; }
[[ -n "$CLI_HB" ]] && [[ "${CLI_HB%.*}" -ge 2 ]] || { echo "ERROR: client heartbeats=$CLI_HB"; echo "$CLI_M"; exit 1; }
[[ -n "$RTT"    ]] && [[ "$RTT" != "0"           ]] || { echo "ERROR: RTT=$RTT"; exit 1; }

echo "E2E test passed (srv_hb=$SRV_HB cli_hb=$CLI_HB rtt=$RTT)"
