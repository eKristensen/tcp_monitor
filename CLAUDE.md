# Code quality

## Use the vendored Rust skills

Before writing, reviewing, or refactoring Rust code in this repository, load both skills
in `.claude/skills/`:

- `rust-best-practices`: Apollo GraphQL's Rust handbook (idioms, errors, testing, linting).
- `pragmatic-rust-guidelines`: Microsoft's Pragmatic Rust Guidelines. Look up guideline
  IDs (e.g. `M-TAUTOLOGICAL-TESTS`) in its `references/all.txt`.

When they conflict with this file, this file wins. Known conflicts:

- Edition stays 2021 (see `Cargo.toml`).
- No `anyhow`/`thiserror`: errors are `String` (config) or `io::Error` (network).
- `TODO` comments reference an entry in `TODO.md` or a GitHub issue.

See `.claude/skills/README.md` for where the skills come from and how to update them.

## Checks that must pass before every commit

```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

CI runs these too, along with the `ci/*.sh` scripts against a release build, and the test
suite on the MSRV toolchain.

## Lint policy

- The lint set lives in `Cargo.toml` `[lints]`, with test exemptions in `clippy.toml`.
  Don't weaken it globally.
- Suppress a lint only at the item that needs it, with
  `#[expect(clippy::lint, reason = "...")]`. `#[allow]` is itself linted.
- Production code has no `unwrap`, `expect`, `panic!`, unchecked indexing/slicing, or `as`
  casts. Use `let ... else`, `get()`, `try_from`, `From`, and so on.
- CI pins the lint toolchain (`toolchain: "1.97"` in `.github/workflows/ci.yml`), so a new
  clippy release cannot break CI on its own. Bump it deliberately and fix new findings in the
  same change.
- CI uses only GitHub's official `actions/*` actions plus the local
  `.github/actions/setup-rust` (rustup + `actions/cache`). Don't add third-party actions.

## MSRV

The MSRV is 1.85, the real floor set by the locked dependencies. It is stated in three places
that must stay in sync: `rust-version` in `Cargo.toml`, `MIN_RUSTC` in `Makefile`, and the
`msrv` job in CI.

## Tests

- Unit tests sit next to the code in `#[cfg(test)] mod tests`, grouped per function under test,
  with names that read as sentences (`rejects_duplicate_peer_names`).
- Integration tests in `tests/` drive the real server and client over loopback. They bind port
  0 and never use fixed ports.
- Wire format and metric names are contracts with other nodes and dashboards. Pin them with
  literal values, not with the code under test (M-TAUTOLOGICAL-TESTS).
- Protocol functions are generic over `AsyncRead`/`AsyncWrite` so they can be tested over
  `tokio::io::duplex`.

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
