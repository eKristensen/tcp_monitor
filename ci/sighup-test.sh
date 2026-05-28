#!/usr/bin/env bash
# SIGHUP tests.
# 1. Normal reload: process must keep running after SIGHUP.
# 2. Bad-config reload: process must survive and keep serving if the config
#    file contains invalid TOML when SIGHUP is received.
# Run from the repository root: ci/sighup-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

# --- test 1: normal reload ---------------------------------------------------
cat > /tmp/sighup.toml << 'TOML'
[node]
name = "ci-reload"
[server]
port         = 19730
metrics_port = 19731
probe_port   = 19732
recv_timeout = 10
TOML

"$BIN" --config /tmp/sighup.toml &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

curl -sf --retry 15 --retry-delay 1 --retry-connrefused http://localhost:19731/metrics > /dev/null
kill -HUP "$PID"
sleep 1
curl -sf http://localhost:19731/metrics > /dev/null

kill "$PID"; wait "$PID" 2>/dev/null || true
trap - EXIT
echo "SIGHUP reload test passed"

# --- test 2: bad config does not crash ---------------------------------------
cat > /tmp/badcfg.toml << 'TOML'
[node]
name = "ci-badcfg"
[server]
port         = 19740
metrics_port = 19741
probe_port   = 19742
recv_timeout = 10
TOML

"$BIN" --config /tmp/badcfg.toml &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

curl -sf --retry 15 --retry-delay 1 --retry-connrefused http://localhost:19741/metrics > /dev/null
echo 'not valid toml = {{{{' > /tmp/badcfg.toml
kill -HUP "$PID"
sleep 1
curl -sf http://localhost:19741/metrics > /dev/null || { echo "ERROR: crashed on bad config"; exit 1; }

echo "Bad-config SIGHUP test passed"
