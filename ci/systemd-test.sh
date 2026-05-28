#!/usr/bin/env bash
# Validates the systemd unit file with systemd-analyze verify.
# Requires sudo and systemd. Run from the repository root: ci/systemd-test.sh
set -euo pipefail

BIN="${TCP_MONITOR_BIN:-./target/release/tcp-monitor}"

sudo cp "$BIN" /usr/local/bin/tcp-monitor
sudo mkdir -p /etc/tcp-monitor
printf '[node]\nname = "ci"\n[server]\nbind = "0.0.0.0"\nport = 9700\nmetrics_port = 9701\nprobe_port = 9702\nrecv_timeout = 90\n' | sudo tee /etc/tcp-monitor/config.toml > /dev/null
systemd-analyze verify tcp-monitor.service
echo "systemd unit validation passed"
