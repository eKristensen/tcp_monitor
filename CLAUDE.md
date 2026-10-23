# Code quality

## Use the vendored Rust skills

Before writing, reviewing, or refactoring Rust code in this repository, load both skills
in `.claude/skills/`:

- `rust-best-practices`: Apollo GraphQL's Rust handbook (idioms, errors, testing, linting).
- `pragmatic-rust-guidelines`: Microsoft's Pragmatic Rust Guidelines. Look up guideline
  IDs (e.g. `M-TAUTOLOGICAL-TESTS`) in its `references/excerpt.txt`.

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
  It is stricter than both vendored guides, which use `warn` and rely on CI's
  `-D warnings`:
  - Every lint is `deny`, so a violation fails `cargo clippy`, and for rustc lints such as
    `missing_docs` even `cargo build`, locally as well as in CI.
  - Lints with no legitimate exception (`unsafe_code`, `dbg_macro`, `todo`,
    `unimplemented`) are `forbid`, which cannot be overridden at all.
  - Enabled on top of the default clippy groups: `pedantic`, `nursery`, `cargo`, the rustc
    lint groups (`rust_2018_idioms`, `unused`, `future_incompatible`, `let_underscore`,
    ...), `missing_docs`, `missing_docs_in_private_items`, and a curated set of restriction
    lints.
  - Don't weaken any of it globally, and don't lower `deny` to `warn`.
- Suppress a lint only at the item that needs it, with
  `#[expect(clippy::lint, reason = "...")]`. `#[allow]` is itself denied.
- Production code has no `unwrap`, `expect`, `panic!`, `process::exit`, `println!`/`eprintln!`,
  unchecked indexing/slicing, or `as` casts. Results are never discarded with `let _ =`:
  handle them, or `drop(...)` them with a comment saying why the outcome doesn't matter.
- Every item, private ones included, has a doc comment.
- Integration test crates start with `#![cfg(test)]` so clippy applies its test
  exemptions to their helper functions too.
- CI pins the lint and test toolchains (`toolchain: "1.97"` in `.github/workflows/ci.yml`), so a
  new clippy or rustc release cannot break CI on its own. Bump both together, deliberately, and
  fix new findings in the same change.
- The one global exception is `multiple_crate_versions = "allow"`: duplicate versions come from
  transitive dependencies we don't control.
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
| `node.name` | Partly — peer sessions started after the reload use the new name; running sessions and server labels keep the old one until restart |
