#!/usr/bin/env python3
"""
tcp_monitor_client.py — Client side of the TCP session longevity monitor.

Connects to the server, sends periodic heartbeat packets, waits for echoes,
measures round-trip time, and exposes Prometheus metrics on a separate HTTP port.
Reconnects automatically after any disconnect so data collection continues.

Disconnect reasons and what they tell you:
  remote_close     — server sent FIN; the remote host deliberately closed the socket
  connection_reset — RST received; abrupt termination from the remote host (process crash,
                     firewall rule) OR a network device injecting a RST into the stream
  timeout          — no echo received within the timeout window; the remote end never
                     sent FIN or RST, so the most likely cause is a silent network drop
                     (packet loss, NAT/firewall session expiry, middlebox)
  local_error      — OS-level socket error on THIS host (interface down, routing failure, etc.)
  connect_failed   — could not establish the TCP connection at all

Usage:
  python3 tcp_monitor_client.py <server-ip> [--port 9700] [--metrics-port 9702]
"""

import argparse
import logging
import signal
import socket
import struct
import sys
import time

from prometheus_client import Counter, Gauge, start_http_server

# ---------------------------------------------------------------------------
# Heartbeat packet layout (16 bytes, big-endian):
#   bytes 0-7  : uint64  sequence number
#   bytes 8-15 : float64 client send timestamp (Unix seconds)
# ---------------------------------------------------------------------------
PACKET_FORMAT = ">Qd"
PACKET_SIZE = struct.calcsize(PACKET_FORMAT)  # 16 bytes

log = logging.getLogger("client")


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

def build_metrics():
    m = {}

    m["session_active"] = Gauge(
        "tcp_session_active",
        "1 if a TCP session is currently established, 0 otherwise",
    )
    m["session_start"] = Gauge(
        "tcp_session_start_timestamp_seconds",
        "Unix timestamp when the current/last session was established",
    )
    m["session_duration"] = Gauge(
        "tcp_session_duration_seconds",
        "Duration in seconds of the active session, or the last completed session",
    )
    m["sessions_total"] = Counter(
        "tcp_sessions_total",
        "Total number of TCP sessions established since startup",
    )
    m["heartbeats_sent"] = Counter(
        "tcp_heartbeats_sent_total",
        "Total heartbeat packets sent",
    )
    m["heartbeats_rx"] = Counter(
        "tcp_heartbeats_received_total",
        "Total heartbeat echoes received from the server",
    )
    m["heartbeats_missed"] = Counter(
        "tcp_heartbeats_missed_total",
        "Total heartbeat echoes that did not arrive within the timeout window",
    )
    m["consecutive_missed"] = Gauge(
        "tcp_heartbeats_consecutive_missed",
        "Current run of consecutive unanswered heartbeats (resets to 0 on any successful echo)",
    )
    m["rtt"] = Gauge(
        "tcp_heartbeat_rtt_seconds",
        "Round-trip time of the most recent heartbeat echo",
    )
    m["last_heartbeat"] = Gauge(
        "tcp_last_heartbeat_timestamp_seconds",
        "Unix timestamp of the most recent successful heartbeat echo",
    )
    m["disconnects"] = Counter(
        "tcp_session_disconnects_total",
        "Session disconnects broken down by reason",
        ["reason"],
    )

    # Pre-initialise all label values so they appear in /metrics from startup.
    for reason in ("remote_close", "connection_reset", "timeout", "local_error", "connect_failed"):
        m["disconnects"].labels(reason=reason)

    return m


# ---------------------------------------------------------------------------
# Socket helpers
# ---------------------------------------------------------------------------

def recv_exact(sock, nbytes):
    """
    Read exactly nbytes from sock.

    Returns bytes on success.
    Raises:
      ConnectionError       — remote sent FIN (recv returned empty)
      ConnectionResetError  — RST received (subclass of ConnectionError)
      TimeoutError          — socket timeout fired
      OSError               — any other socket-level error
    """
    buf = bytearray()
    while len(buf) < nbytes:
        chunk = sock.recv(nbytes - len(buf))
        if not chunk:
            raise ConnectionError("remote closed the connection")
        buf += chunk
    return bytes(buf)


def set_keepalive(sock):
    """Enable TCP keepalives as a belt-and-suspenders safety net."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)


# ---------------------------------------------------------------------------
# Session runner
# ---------------------------------------------------------------------------

def run_session(sock, server_addr, metrics, args):
    """
    Drive one established connection.
    Sends heartbeats on schedule, waits for echoes, updates metrics.
    Returns the disconnect reason string when the session ends.
    """
    start_time = time.time()
    metrics["session_active"].set(1)
    metrics["session_start"].set(start_time)
    metrics["sessions_total"].inc()
    metrics["consecutive_missed"].set(0)

    log.info("Session established to %s:%d", server_addr[0], server_addr[1])

    # Socket timeout governs how long we wait for an echo after each send.
    # We allow one full heartbeat interval — the echo should come back in
    # milliseconds normally, but this gives generous headroom.
    sock.settimeout(args.heartbeat_interval)

    seq = 0
    consecutive_missed = 0
    reason = "local_error"

    # Use a monotonic clock for scheduling sends so wall-clock adjustments
    # (NTP, daylight saving) do not affect the interval.
    next_send = time.monotonic()

    try:
        while True:
            # -----------------------------------------------------------------
            # 1. Sleep until it is time to send the next heartbeat.
            # -----------------------------------------------------------------
            now_mono = time.monotonic()
            gap = next_send - now_mono
            if gap > 0:
                time.sleep(gap)
            next_send += args.heartbeat_interval

            # -----------------------------------------------------------------
            # 2. Send heartbeat.
            # -----------------------------------------------------------------
            send_ts = time.time()
            payload = struct.pack(PACKET_FORMAT, seq, send_ts)

            try:
                sock.sendall(payload)
            except TimeoutError:
                # The send itself timed out — treat as a missed heartbeat.
                consecutive_missed += 1
                metrics["heartbeats_missed"].inc()
                metrics["consecutive_missed"].set(consecutive_missed)
                log.warning("Send timeout (consecutive misses: %d/%d)", consecutive_missed, args.max_misses)
                if consecutive_missed >= args.max_misses:
                    reason = "timeout"
                    break
                continue
            except ConnectionResetError:
                reason = "connection_reset"
                break
            except ConnectionError:
                reason = "remote_close"
                break
            except OSError as exc:
                reason = "local_error"
                log.warning("Send error: %s", exc)
                break

            metrics["heartbeats_sent"].inc()
            log.debug("Sent seq=%d", seq)
            seq += 1

            # -----------------------------------------------------------------
            # 3. Wait for echo.
            # -----------------------------------------------------------------
            try:
                echo = recv_exact(sock, PACKET_SIZE)
                _, echo_ts = struct.unpack(PACKET_FORMAT, echo)
                rtt = time.time() - echo_ts

                consecutive_missed = 0
                metrics["consecutive_missed"].set(0)
                metrics["heartbeats_rx"].inc()
                metrics["rtt"].set(rtt)
                metrics["last_heartbeat"].set(time.time())
                metrics["session_duration"].set(time.time() - start_time)

                log.debug("Echo received  rtt=%.3fs", rtt)

            except TimeoutError:
                consecutive_missed += 1
                metrics["heartbeats_missed"].inc()
                metrics["consecutive_missed"].set(consecutive_missed)
                log.warning(
                    "Heartbeat echo timeout (consecutive misses: %d/%d)",
                    consecutive_missed, args.max_misses,
                )
                if consecutive_missed >= args.max_misses:
                    reason = "timeout"
                    log.error("Max consecutive misses reached — closing session")
                    break

            except ConnectionResetError:
                reason = "connection_reset"
                break
            except ConnectionError:
                reason = "remote_close"
                break

    except OSError as exc:
        reason = "local_error"
        log.warning("Unexpected socket error: %s", exc)

    duration = time.time() - start_time
    metrics["session_active"].set(0)
    metrics["session_duration"].set(duration)
    metrics["disconnects"].labels(reason=reason).inc()

    log.info("Session ended  duration=%.1fs  reason=%s", duration, reason)
    return reason


# ---------------------------------------------------------------------------
# Connect-and-run loop
# ---------------------------------------------------------------------------

def run(args, metrics):
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_keepalive(sock)

        log.info("Connecting to %s:%d ...", args.host, args.port)
        try:
            sock.connect((args.host, args.port))
        except OSError as exc:
            log.error("Connection failed: %s", exc)
            metrics["disconnects"].labels(reason="connect_failed").inc()
            sock.close()
            log.info("Retrying in %ds", args.reconnect_delay)
            time.sleep(args.reconnect_delay)
            continue

        run_session(sock, (args.host, args.port), metrics, args)

        try:
            sock.close()
        except OSError:
            pass

        log.info("Reconnecting in %ds ...", args.reconnect_delay)
        time.sleep(args.reconnect_delay)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="TCP session monitor — client side",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("host",
                        help="Server hostname or IP address")
    parser.add_argument("--port", type=int, default=9700,
                        help="TCP test port (must match server --port)")
    parser.add_argument("--metrics-port", type=int, default=9702,
                        help="Prometheus metrics HTTP port")
    parser.add_argument("--heartbeat-interval", type=float, default=30.0,
                        help="Seconds between heartbeats")
    parser.add_argument("--max-misses", type=int, default=3,
                        help="Consecutive missed echoes before declaring the session dead")
    parser.add_argument("--reconnect-delay", type=float, default=10.0,
                        help="Seconds to wait before reconnecting after a disconnect")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    metrics = build_metrics()
    start_http_server(args.metrics_port)
    log.info("Prometheus metrics available at http://0.0.0.0:%d/metrics", args.metrics_port)

    run(args, metrics)


if __name__ == "__main__":
    main()
