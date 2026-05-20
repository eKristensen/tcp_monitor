#!/usr/bin/env python3
"""
tcp_monitor_server.py — Server side of the TCP session longevity monitor.

Accepts one TCP connection at a time, echoes heartbeat packets back,
and exposes Prometheus metrics on a separate HTTP port.

Disconnect reasons and what they tell you:
  remote_close     — client sent FIN; the remote host deliberately closed the socket
  connection_reset — RST received; abrupt termination from the remote host (process crash,
                     firewall rule) OR a network device injecting a RST into the stream
  timeout          — no heartbeat received within the timeout window; the remote end never
                     sent FIN or RST, so the most likely cause is a silent network drop
                     (packet loss, NAT/firewall session expiry, middlebox)
  local_error      — OS-level socket error on THIS host (interface down, routing failure, etc.)

Usage:
  python3 tcp_monitor_server.py [--bind 0.0.0.0] [--port 9700] [--metrics-port 9701]
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

log = logging.getLogger("server")


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
        "Total number of TCP sessions accepted since startup",
    )
    m["heartbeats_rx"] = Counter(
        "tcp_heartbeats_received_total",
        "Total heartbeat packets received from the client",
    )
    m["last_heartbeat"] = Gauge(
        "tcp_last_heartbeat_timestamp_seconds",
        "Unix timestamp of the most recently received heartbeat",
    )
    m["disconnects"] = Counter(
        "tcp_session_disconnects_total",
        "Session disconnects broken down by reason",
        ["reason"],
    )

    # Pre-initialise all label values so they appear in /metrics from startup,
    # even before the first disconnect event.
    for reason in ("remote_close", "connection_reset", "timeout", "local_error"):
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
      TimeoutError          — socket timeout fired (sock.settimeout was set)
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
    # Linux-specific tuning: start probing after 60 s of idle, then every 10 s,
    # give up after 6 failed probes (total 60 s of probing before the OS kills it).
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)


# ---------------------------------------------------------------------------
# Session handler
# ---------------------------------------------------------------------------

def handle_session(conn, addr, metrics, recv_timeout):
    """
    Drive one accepted connection until it ends.
    Echoes every heartbeat packet back unchanged and updates metrics.
    """
    start_time = time.time()
    metrics["session_active"].set(1)
    metrics["session_start"].set(start_time)
    metrics["sessions_total"].inc()

    log.info("Session started from %s:%d", addr[0], addr[1])

    set_keepalive(conn)
    # If no heartbeat arrives within recv_timeout seconds we declare the
    # session timed out.  Default: heartbeat_interval × 3.
    conn.settimeout(recv_timeout)

    reason = "local_error"

    try:
        while True:
            data = recv_exact(conn, PACKET_SIZE)
            seq, client_ts = struct.unpack(PACKET_FORMAT, data)
            now = time.time()

            metrics["heartbeats_rx"].inc()
            metrics["last_heartbeat"].set(now)
            metrics["session_duration"].set(now - start_time)

            log.debug("heartbeat seq=%d client_ts=%.3f age=%.3fs", seq, client_ts, now - client_ts)

            # Echo the packet back unchanged so the client can measure RTT.
            conn.sendall(data)

    except ConnectionResetError:
        # RST: abrupt termination — remote host or network device
        reason = "connection_reset"
    except ConnectionError:
        # Empty recv: clean FIN from remote host
        reason = "remote_close"
    except TimeoutError:
        # No heartbeat within recv_timeout: most likely a silent network drop
        reason = "timeout"
    except OSError as exc:
        reason = "local_error"
        log.warning("Socket error: %s", exc)

    duration = time.time() - start_time
    metrics["session_active"].set(0)
    metrics["session_duration"].set(duration)
    metrics["disconnects"].labels(reason=reason).inc()

    log.info("Session ended  duration=%.1fs  reason=%s", duration, reason)

    try:
        conn.close()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(args, metrics):
    recv_timeout = args.heartbeat_interval * args.timeout_multiplier

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.bind, args.port))
    srv.listen(1)

    log.info(
        "Listening on %s:%d  recv_timeout=%.0fs  metrics=http://0.0.0.0:%d/metrics",
        args.bind, args.port, recv_timeout, args.metrics_port,
    )

    while True:
        try:
            conn, addr = srv.accept()
        except OSError as exc:
            log.error("Accept error: %s — retrying in 1 s", exc)
            time.sleep(1)
            continue

        handle_session(conn, addr, metrics, recv_timeout)


def main():
    parser = argparse.ArgumentParser(
        description="TCP session monitor — server side",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bind", default="0.0.0.0",
                        help="IP address to bind the test socket")
    parser.add_argument("--port", type=int, default=9700,
                        help="TCP test port")
    parser.add_argument("--metrics-port", type=int, default=9701,
                        help="Prometheus metrics HTTP port")
    parser.add_argument("--heartbeat-interval", type=float, default=30.0,
                        help="Expected client heartbeat interval in seconds")
    parser.add_argument("--timeout-multiplier", type=float, default=3.0,
                        help="Declare timeout after this many missed heartbeat intervals")
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
