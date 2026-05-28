#!/usr/bin/env bash
# Smoke test: server-only mode.
# Checks that /metrics responds and the probe port sends the right banner.
# Run from the repository root: ci/smoke-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

cat > /tmp/smoke.toml << 'TOML'
[node]
name = "ci-smoke"
[server]
port         = 19700
metrics_port = 19701
probe_port   = 19702
recv_timeout = 10
TOML

"$BIN" --config /tmp/smoke.toml &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

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
echo "Smoke test passed"
