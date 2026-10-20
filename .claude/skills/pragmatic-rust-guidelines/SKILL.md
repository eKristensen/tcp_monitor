---
name: pragmatic-rust-guidelines
description: >
  Microsoft's Pragmatic Rust Guidelines, the condensed edition Microsoft publishes for AI coding
  agents. Use this skill when writing, reviewing, or refactoring Rust code in this repository:
  error handling and panics, logging, testing (including avoiding tautological tests), lint
  overrides, documentation, MSRV, API shape, and performance.
license: MIT
metadata:
  author: microsoft
  source: https://github.com/microsoft/rust-guidelines
---

# Pragmatic Rust Guidelines (Microsoft)

The full guideline text is in [references/all.txt](references/all.txt) (about 3,500 lines).
Each guideline has an ID such as `M-PANIC-IS-STOP`. Find one with
`grep -n "M-PANIC-IS-STOP" references/all.txt` and read that section, instead of loading the whole file.

This file is Microsoft's own agent edition, unmodified. Only this `SKILL.md` wrapper is
local; see `../README.md` for provenance and how to update it.

## Guidelines that matter most for tcp-monitor

tcp-monitor is a small application crate (async networking, Prometheus metrics), so the
`Application`, `Correctness`, `Universal`, and `AI` sections apply directly. The `Library`,
`FFI`, and `Macros` sections mostly do not.

| ID | Rule |
|----|------|
| M-TAUTOLOGICAL-TESTS | Tests check behaviour, not values restated from the implementation |
| M-PANIC-IS-STOP | A panic means "stop the program"; never use it for recoverable errors |
| M-PANIC-ON-BUG | Detected programming bugs are panics, not errors |
| M-LINT-OVERRIDE-EXPECT | Suppress lints with `#[expect(..., reason = "...")]`, never `#[allow]` |
| M-STATIC-VERIFICATION | Lint set used in `Cargo.toml` `[lints]` |
| M-DOCUMENTED-MAGIC | Magic values (timeouts, caps, sizes) are named and documented |
| M-LOG-STRUCTURED | Structured logging with named fields |
| M-MOCKABLE-SYSCALLS | I/O is abstracted so it can be tested (cf. generic `AsyncRead`/`AsyncWrite` in `protocol.rs`) |
| M-INTEGRATION-TESTS | Integration tests live under `tests/` |
| M-MSRV | MSRV is conservatively updated |
| M-RUST-SHAPED | Rust code solves Rust problems; no patterns ported from other languages |
| M-DESIGN-FOR-AI | Strong types, docs, and tests make code safer to change, for humans and agents |

## Where this conflicts with the repository

Repository rules in `CLAUDE.md` win. Known differences:

- **Edition**: M-LATEST-EDITION asks for the latest edition. The crate stays on 2021; see the
  comment in `Cargo.toml`.
- **Errors**: M-APP-ERROR allows `anyhow`. The crate uses `String` and `io::Error` and has no
  `anyhow` dependency. Don't add one without a reason.
