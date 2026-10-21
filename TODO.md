# TODO and ideas

Ideas that are being considered but not yet decided are marked **Idea**. Don't implement
these without a decision first.

## Idea: test large payloads alongside the 16-byte heartbeat

**Goal.** Check that full-size packets get through a cloud provider network we don't control,
not just tiny ones. Report "small works / large works" as two separate answers.

**What TCP actually puts on the wire** (this matters for the design):

- Each TCP segment on the wire is at most the MSS: MTU − 40 bytes of IPv4+TCP headers, or
  1448 bytes on a 1500 MTU link with the TCP timestamp option.
- The size of the application's write does not matter beyond that. The kernel splits a large
  write into MSS-sized segments.
- With TSO/GSO the kernel may hand the NIC "segments" of up to 64 KiB, which the NIC slices
  into MSS-sized frames. With GRO, the receiver merges frames back together. `tcpdump` on a
  host can therefore show very large TCP segments, but nothing larger than the path MTU ever
  crosses the wire.
- Linux TCP sets Don't Fragment, so TCP traffic is normally **not IP-fragmented**. When a
  smaller-MTU hop is on the path (overlay/VXLAN/GENEVE encapsulation, VPN, tunnel), one of
  three things happens:
  1. Path MTU discovery works: an ICMP "fragmentation needed" reply comes back and the
     sender lowers its MSS.
  2. **MTU black hole**: the ICMP reply is filtered, so full-size segments vanish while small
     packets still pass. The session looks alive, but bulk transfers stall. This is the failure
     worth detecting.
  3. A middlebox clamps the MSS during the handshake, and everything works at the smaller size.
- The endpoints' own packets are only IP-fragmented with UDP, or with DF cleared on the
  socket. Testing that would need a UDP port (a new firewall rule) or unsafe socket options.
  Note, though, that "DF set" does not mean "no fragments anywhere on the path": an overlay
  tunnel (VXLAN/GENEVE/GRE) that doesn't copy DF to the outer header may fragment the
  *outer* packet when an inner full-size segment plus encapsulation exceeds the underlay MTU.
  Many cloud networks and middleboxes handle fragments poorly, so that case can show up as
  loss or poor throughput on full-size segments only.

So a payload of **anything larger than one MSS (more than about 1448 bytes)** is enough to
exercise full-size packets. A few KiB is plenty; 64 KiB only spreads the same test over more
segments.

**Design sketch.**

- A second TCP connection per peer to the **existing heartbeat port**, marked by a new
  handshake type, so no new port is needed. It must be a separate connection: a stalled large
  frame would otherwise block the small heartbeats queued behind it, and the two results would
  no longer be independent.
- Frame: `seq + timestamp + length + payload`. The payload pattern is derived from `seq` so the
  server can verify it without a checksum dependency. The server echoes the whole frame, which
  tests both directions.
- Optional, off by default: `[client.large_payload] enabled / size / interval`.
- Server-side size cap as a constant. Adding a new `[server]` field would break existing
  configs, because `[server]` fields are required.
- Metrics: new `tcp_monitor_*_large_*` families, so existing metric names don't change.
- Requires a backward-compatible handshake v2: a magic `u32` prefix larger than 256, so old
  servers reject it cleanly and new servers still accept old clients.

**Test idea without opening ports.** Use network namespaces in CI: `A ─1500─ R ─1280→ B`, where
R's end of the R–B link has MTU 1280 and B's end 1500 (so both endpoints advertise MSS 1460),
and R drops ICMP "frag needed". Expect small heartbeats to keep working and the large test to
fail. After removing the drop rule, expect the large test to recover.

**Related, possibly more useful for troubleshooting cloud TCP performance.** Export the kernel's
own per-socket TCP statistics (`TCP_INFO`) for every heartbeat session: smoothed RTT and its
variance, retransmits, lost segments, congestion window, and the MSS/path MTU the kernel
actually uses. This adds zero traffic and shows directly whether PMTUD lowered the MSS and
whether packets are being lost. It needs a crate that wraps `getsockopt(TCP_INFO)`, since
the crate forbids `unsafe`.

## Idea: optional peer-to-peer mesh with peer discovery (gossip)

**Open question.** Does automatic discovery defeat the purpose of the monitor? Today the config
says exactly which paths should exist. Discovery would hide a missing path instead of reporting
it.

**Design sketch, if pursued.**

- Gossip runs as a short-lived connection type on the existing heartbeat port: swap peer lists,
  then close. It never touches the long-lived heartbeat sessions.
- Static `[[peers]]` act as seeds. Peer addresses come from the observed source IP plus the
  `heartbeat_port` sent in the handshake. `advertise_host` overrides the address for NAT.
- Each entry carries an age. Merging keeps the lowest age, entries expire after
  `forget_after`, and static peers win on a name clash.
- Safety: `allowed_networks` (CIDR allowlist), a `cluster` name checked in the handshake, and
  `max_peers` (which also bounds metric label cardinality). Without these, anyone who can
  connect could make nodes connect out to arbitrary hosts.
- Optional, off by default (`[mesh] enabled = false`), and hot-reloadable through the existing
  peer manager.
- Test: A knows B, B knows C → A must end up with a session to C, and it must expire after C
  stops.

## Smaller follow-ups noticed while adding tests

- **Structured logging** (Microsoft M-LOG-STRUCTURED): logs use `info!("... {}", x)` string
  formatting. Switch to named fields (`info!(peer = %name, "session started")`) so journald
  and log pipelines can filter on them.
- **Server sessions outlive shutdown**: cancelling `server::run` stops the accept loops, but
  sessions that were already spawned keep running until the process exits. This is harmless
  for the binary, but means a clean in-process shutdown is not possible.
- **Handshake name truncation** cuts at 256 bytes, which can split a multi-byte UTF-8
  character; the server then labels the peer `unknown`. Truncate on a character boundary.
- **Metrics bind address**: the metrics endpoint always binds `0.0.0.0`, ignoring
  `server.bind`.
- **Echo reads are not cancellation-safe.** `timeout_at(.., recv_heartbeat)` in the client
  drops a half-finished read when the deadline fires. If a 16-byte packet is split across
  segments at that instant, the stream desynchronises (seq and timestamp misaligned) for the
  rest of the session. This is pre-existing (the original `read_exact` had the same
  property). Fix: read into a per-session buffer that survives the timeout.
