# mavlink-router-rs — Rust MAVLink Router

A minimal, high-throughput MAVLink router in Rust. "RMR" is the short-form alias used throughout this doc, in body prose, doc comments, and log messages — the project's canonical name is `mavlink-router-rs`.

This document is the working brief — charter, philosophy, and conventions. Implementation details and design rationale live as rustdoc / module-level documentation in the source, not here.

## What RMR is

A transparent forwarder of MAVLink frames between multiple endpoints (serial / UDP / TCP). It owns no MAVLink identity, originates no traffic, and parses no more of a frame than it needs to in order to route it.

## What RMR is NOT (hard non-goals)

These are deliberate exclusions. **Do not add them.** If a use case demands one, it belongs in a separate tool (a GCS, a logger sidecar, etc.) — not in RMR.

- **No HEARTBEAT emission.** RMR is not a node. It must never appear on the bus with a sysid/compid of its own.
- **No `REQUEST_DATA_STREAM` / `MAV_CMD_SET_MESSAGE_INTERVAL`.** Stream rate is a policy decision between vehicle and GCS. The router stays out of it.
- **No parameter, mission, rally, fence, or terrain fetching.**
- **No plugin/module system.** No map, ftp, signing, joystick, etc.
- **No identity flags.** No `--source-system`, `--source-component`.
- **No `.tlog` recording by default.** May be added as an opt-in side feature, but the router's primary job is forwarding.
- **No MAVLink signing generation or validation.** Signed frames pass through as opaque bytes.
- **No dialect translation between v1 and v2.** Both versions are supported on the wire; frames are forwarded in their original version.
- **No message rewriting.** sysid/compid/seq are never modified in-flight.
- **No interactive console, REPL, or curses UI.**
- **No GUI, no web dashboard, no REST API.** A stats endpoint (later) is fine; an admin API is not.

## Philosophy

1. **Header-only parsing.** A router does not need typed messages. Parse the MAVLink header (sysid, compid, msgid, seq, payload length, version) and — for the ~20 messages that carry targeting fields — extract `target_system` / `target_component` at known offsets. Nothing else.
2. **Forward what you don't understand.** Unknown msgids (dialects you weren't compiled with) MUST be forwarded as broadcast. CRC validation is skipped for them. This is what `mavlink-router` does and it is the right call.
3. **Dialect-agnostic by default; dialect-aware when it helps.** The router ships with a built-in table covering `common.xml` + `ardupilotmega.xml`, compiled in at build time. Frames with unknown msgids still forward — as broadcast, without CRC check. **No frame is ever rejected for being unknown.** Integrators who need targeted routing or CRC validation for a custom dialect fork the repo, drop their XML next to the vendored files, and add the filename to the `DIALECTS` list in `build.rs` — the rest of the table is regenerated automatically.
4. **No allocation in the hot path.** Per-endpoint fixed-size read buffers. Frames cross task boundaries as `bytes::Bytes` with zero-copy reference-counted clones to each writer.
5. **Cross-platform from day one.** Every design choice must work on Windows and Linux. macOS comes free if those two do. No `epoll`-only abstractions.
6. **Single binary, simple CLI.** Endpoints declared as scheme-prefixed strings on the command line (mavp2p style). A TOML config file is supported for larger setups but not required.
7. **Fail loud, not silent.** A failed reconnect logs at WARN; CRC errors are counted per endpoint; routing decisions can be traced at DEBUG.
8. **Never give up on an endpoint.** Bind, open, and connect failures retry forever on a capped-exponential backoff with jitter — there is no attempt limit and no terminal "gave up" state. A serial cable unplugged at boot, a TCP peer that comes online an hour later, a UDP bind that loses to a port-stealer all recover on their own. Shutdown is the only exit; the cancel token trips and the retry loop unwinds.
9. **Connection lifecycle is visible to operators at INFO.** Every endpoint logs `listening`, `connected`, `opened`, `disconnected`, and `reconnecting` at INFO inside its per-endpoint span, so `rmr` run without `-v` still tells an operator which links came up, which dropped, and which are flapping. WARN/DEBUG/TRACE carry detail; INFO carries the flow.

## Backlog

Post-v1 work, not currently scheduled. Each item carries its own design notes; pull into a phased plan when picked up.

- Optional `.tlog` recording per endpoint or globally
- Bandwidth shaping / rate limiting per endpoint
- Prometheus stats endpoint
- **Two-class priority TX queue per endpoint** (`low_priority_msgids` config list, weighted drain ratio configurable, default 4:1). High class is the existing single queue; low class is added alongside. No reordering within a class. The retrofit must not change `TxQueue`'s appearance in `EndpointEvent::EndpointAdded`/`PeerAdded` or in any `*Wiring` struct — internals switch to two `ArrayQueue<Bytes>` + weighted drain, the API gains `push_low(b)`, and existing `push(b)` keeps "high-class" semantics so existing callers are correct by default.
- **Dynamic endpoints / hot config reload.** Add and remove endpoints at runtime without restarting the router. SIGHUP on Unix and a file-watcher on Windows trigger a TOML re-parse; the diff against the live set yields add / remove / modify lists, executed via the existing lifecycle channel (extending `EndpointEvent` with an `EndpointRemoved` variant for top-level endpoints alongside the existing `PeerRemoved`). `IdentityFlags` switches from by-value clones to `ArcSwap<IdentityFlags>` at every holder so live filters can be retargeted without endpoint teardown.
- **Stats visualizer.** Separate companion CLI / TUI that consumes RMR's JSON-Lines stdout (or follows a log file) and renders per-endpoint throughput, queue depth, drop counters, and state transitions over time. Not part of the router binary; ships as its own crate or as a script under `tools/`.

## Testing strategy

**Every module ships with tests. Every PR adds tests for the surface it touches. A bug fix without a regression test is incomplete.**

Three test tiers:

1. **Unit tests** — colocated in the source file via `#[cfg(test)] mod tests { ... }`. Fast (< 10ms each), no I/O, no Tokio runtime required (or only `#[tokio::test(flavor = "current_thread")]` for the few async helpers).
2. **Property tests** — `proptest`, also colocated. Required for the framer ("garbage in, no panic, no infinite loop, always resync within N bytes") and the filter evaluator ("blocklist always wins over allowlist on overlap"). Catches the classes of bugs example-based tests miss.
3. **Integration tests** — `tests/*.rs`. Spin up real sockets on `127.0.0.1` using `tokio::net::TcpListener::bind("127.0.0.1:0")` (or the UDP equivalent) for an OS-assigned port. Drive transports end-to-end. **No I/O mocks** — mocking hides exactly the reconnect, timeout, and partial-read bugs that integration tests exist to catch.

Binary-driven e2e tests use `assert_cmd` + `predicates` to spawn `rmr` and assert on stderr/stdout patterns. Signal-delivery on Windows has no portable `assert_cmd` analogue to Unix `SIGTERM`, so binary-driven shutdown cases are `#[cfg(unix)]` only — Windows shutdown coverage runs through the `lib::run` path.

Fixtures live in `fixtures/` and are version-controlled. Capture binaries should be small (< 1 MB each) and reproducible.

CI runs `cargo +nightly fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-features` on every push — all three are required to pass.

## Conventions for this repo

- Default to no comments. Names should carry the meaning; comments are for non-obvious WHY.
- **Every struct gets a brief `///` docstring** explaining its purpose — what it represents and why it exists, not how it's used. This is API documentation exempt from the "no comments" rule above. One or two sentences. Self-explanatory fields stay undocumented.
- **Documentation lives in the source, not here.** Module-level rustdoc (`//!`) is the right home for "how this subsystem works" prose. Struct and function rustdoc (`///`) is the right home for "what this represents and why it's shaped this way" rationale. CLAUDE.md is a working brief, not a design reference.
- One `tracing::span` per endpoint; route decisions log at `trace!`.
- Errors are `thiserror` enums per module, joined at the binary boundary by a top-level `Error` enum with `From` impls. **No `anyhow`** anywhere in the tree, including `main`.
- No `unwrap()` outside of `main()` startup and tests.
- Bounded channels everywhere on the data path; dropped messages are counted, not buffered indefinitely.
- **Add new functionality to the existing module that owns the concern** — do not create new top-level modules without a clear reason.
- **Tests are not optional.** Every PR adds tests for the surface it touches. A bug fix without a regression test is incomplete.
- **`cargo fmt --check` and `cargo clippy -- -D warnings` are required to pass.** Style and lint regressions fail CI; fix them locally before pushing.
- **Clippy `all` group is denied at the manifest level** via `[lints.clippy] all = { level = "deny", priority = -1 }` in `Cargo.toml`, so the full `clippy::all` group is enforced on every `cargo build`/`check`/`clippy`, not only when CI passes `-- -D warnings`. Per-site `#[allow(clippy::<lint>)]` still works (the deny is at priority `-1`).
- **Releases are tag-driven.** Run `scripts/release.sh {major|minor|patch}` to cut a release; the tag push fires `.github/workflows/release.yml`, which is the only path that produces artifacts. Merges to main never invoke it.
