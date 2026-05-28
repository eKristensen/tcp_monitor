#!/usr/bin/env bash
# Server startup test: server-only mode.
# Checks that /metrics responds and the probe port sends the right banner.
# Run from the repository root: ci/server-startup-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

cat > /tmp/server-startup.toml << 'TOML'
[node]
name = "ci-startup"
[server]
bind         = "0.0.0.0"
port         = 19700
metrics_port = 19701
probe_port   = 19702
recv_timeout = 10
TOML

"$BIN" --config /tmp/server-startup.toml &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

# Wait for the metrics port to be ready before proceeding.
# -s: silent  -f: fail on HTTP error  --retry-connrefused: also retry on ECONNREFUSED
# (the process may not have bound the port yet when this runs)
curl -sf --retry 15 --retry-delay 1 --retry-connrefused \
  http://localhost:19701/metrics > /dev/null

# Python avoids OpenBSD netcat's early-exit-on-stdin-EOF and \r issues.
BANNER=$(python3 - << 'PYEOF'
import socket, sys
try:
    with socket.create_connection(('127.0.0.1', 19702), timeout=5) as s:
        print(s.recv(64).decode().strip())
except Exception as e:
    print('CONNECT_ERROR:', e, file=sys.stderr); sys.exit(1)
PYEOF
)

[[ "$BANNER" == "TCP-MONITOR OK" ]] || { echo "ERROR: banner='$BANNER'"; exit 1; }
echo "Server startup test passed"
