# Design choices

## Config: explicit server fields, optional client/peer fields

`[server]` fields are all required — no `serde` defaults. Every value the
server needs to bind ports and time out sessions must be stated explicitly in
the config file so the operator knows exactly what is running.

`[client]` and `[[peers]]` fields may carry `serde` defaults for values that
have a clear, universally sensible fallback (e.g. `heartbeat_port = 9700` for peers,
`heartbeat_interval = 30` for the client). These are convenience defaults that
reduce boilerplate in common deployments; they are documented in
`config.example.toml`.

## What hot-reloads on SIGHUP vs. what requires restart

| Field | Behaviour |
|-------|-----------|
| `server.heartbeat_recv_timeout` | Hot-reloads — applies to new sessions |
| `server.probe_idle_timeout` | Hot-reloads — applies to new probe connections |
| `server.bind`, `heartbeat_port`, `probe_port`, `metrics_port` | Requires restart — warns on reload |
| `[client]` timing fields | Requires restart — warns on reload |
| `[[peers]]` add / remove / address change | Hot-reloads — tasks started/stopped/restarted |
| `node.name` | Hot-reloads — read from live config each reload cycle |
