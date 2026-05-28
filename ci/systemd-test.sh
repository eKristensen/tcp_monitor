#!/usr/bin/env bash
# Validates the systemd unit file with systemd-analyze verify.
# Requires sudo and systemd. Run from the repository root: ci/systemd-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

sudo cp "$BIN" /usr/local/bin/tcp-monitor
sudo mkdir -p /etc/tcp-monitor
sudo tee /etc/tcp-monitor/config.toml > /dev/null << 'TOML'
[node]
name = "ci"

[server]
bind         = "0.0.0.0"
port         = 9700
probe_port   = 9701
metrics_port = 9702
heartbeat_recv_timeout = 90
probe_idle_timeout     = 30
TOML
systemd-analyze verify tcp-monitor.service
echo "systemd unit validation passed"
