# RMR — Rust MAVLink Router

A minimal, high-throughput MAVLink router in Rust. Inspired by `mavlink-router` (Intel) in architecture, by `mavp2p` (Bluenviron) in portability and CLI surface, and by `MAVProxy` only as a list of things **not** to do.

This document is the working brief. Read it before changing core behavior; update it when the design changes.

## What RMR is

A transparent forwarder of MAVLink frames between multiple endpoints (serial / UDP / TCP). It owns no MAVLink identity, originates no traffic, and parses no more of a frame than it needs to in order to route it.

Think of it as a bump in the wire: bytes in, bytes out, with a learned routing table that improves targeted delivery over pure broadcast.

## What RMR is NOT (hard non-goals)

These are deliberate exclusions. **Do not add them.** If a use case demands one, it belongs in a separate tool (a GCS, a logger sidecar, etc.) — not in RMR.

- **No HEARTBEAT emission.** RMR is not a node. It must never appear on the bus with a sysid/compid of its own. (MAVProxy and mavp2p both emit heartbeats by default; this is the single most common cause of GCS-failsafe misbehavior when a router is in the path.)
- **No `REQUEST_DATA_STREAM` / `MAV_CMD_SET_MESSAGE_INTERVAL`.** Stream rate is a policy decision between vehicle and GCS. The router stays out of it.
- **No parameter, mission, rally, fence, or terrain fetching.**
- **No interactive console, REPL, or curses UI.**
- **No plugin/module system.** No map, ftp, signing, joystick, etc.
- **No identity flags.** No `--source-system`, `--source-component`.
- **No `.tlog` recording by default.** May be added as an opt-in side feature, but the router's primary job is forwarding.
- **No MAVLink signing generation or validation.** Signed frames pass through as opaque bytes.
- **No dialect translation between v1 and v2.** Both versions are supported on the wire; frames are forwarded in their original version.
- **No message rewriting.** sysid/compid/seq are never modified in-flight.
- **No GUI, no web dashboard, no REST API.** A stats endpoint (later) is fine; an admin API is not.

## Philosophy

1. **Header-only parsing.** A router does not need typed messages. Parse the MAVLink header (sysid, compid, msgid, seq, payload length, version) and — for the ~20 messages that carry targeting fields — extract `target_system` / `target_component` at known offsets. Nothing else.
2. **Forward what you don't understand.** Unknown msgids (dialects you weren't compiled with) MUST be forwarded as broadcast. CRC validation is skipped for them. This is what `mavlink-router` does and it is the right call.
3. **Dialect-agnostic by default; dialect-aware when it helps.** The router ships with a built-in table covering `common.xml` + `ardupilotmega.xml`, compiled in at build time. Frames with unknown msgids still forward — as broadcast, without CRC check. **No frame is ever rejected for being unknown.** Integrators who need targeted routing or CRC validation for a custom dialect fork the repo, drop their XML next to the vendored files, and add the filename to the `DIALECTS` list in `build.rs` — the rest of the table is regenerated automatically.
4. **No allocation in the hot path.** Per-endpoint fixed-size read buffers. Frames cross task boundaries as `bytes::Bytes` — one allocation when the framer freezes a slice out of its `BytesMut` accumulator (standard `tokio_util` codec pattern: `split_to(frame_len).freeze()` then `reserve(n)` to top up capacity), then zero-copy reference-counted clones to each writer.
5. **Cross-platform from day one.** Every design choice must work on Windows and Linux. macOS comes free if those two do. No `epoll`-only abstractions.
6. **Single binary, simple CLI.** Endpoints declared as scheme-prefixed strings on the command line (mavp2p style). A TOML config file is supported for larger setups but not required.
7. **Fail loud, not silent.** A failed reconnect logs at WARN; CRC errors are counted per endpoint; routing decisions can be traced at DEBUG.

## Architecture

### Component overview

A single binary, four task roles in Tokio (per-endpoint reader, per-endpoint writer, one router, one stats). State ownership is strict — no shared mutable state across tasks; everything flows through bounded channels (`tokio::sync::mpsc` for reader→router with backpressure, lock-free `crossbeam_queue::ArrayQueue` for router→writer with drop-oldest, dedicated `mpsc<StatsEvent>` + `mpsc<StatsLine>` for the stats task).

```
                                    main.rs
                  parse CLI ─► load TOML ─► init tracing
                                       │
                                       ▼
                   spawn Router + Stats tasks ─► spawn N × {Reader, Writer} per endpoint
                                       │
                                       ▼
                              wait on shutdown signal


       Endpoint reader task                  Router task                  Endpoint writer task
       ────────────────────                  ───────────                  ────────────────────
       ▸ read transport bytes                ▸ owns learn table          ▸ pop from TX queue
       ▸ feed framer state machine           ▸ owns dedup window         ▸ write to transport
       ▸ on full frame:                      ▸ owns per-destination      ▸ on disconnect:
         parse header,                         decision state               drain + discard
         CRC validate (known msgids),        ▸ receive frames
         in-filter (drop + count),           ▸ force_push to TX,
         emit (Id, Bytes, ParsedHeader)        drop-oldest on overflow
                          │                       │                          ▲
                          └─ mpsc (backpressure) ►│                          │
                                                  │                          │
                                  ingress: dedup ─┤                          │
                                          learn ──┤                          │
                                                  │                          │
                          per destination:        │                          │
                                   loop-prevent ──┤                          │
                                   sniffer? ──────┤                          │
                                   out-filter ────┤                          │
                                   target-match ──┤                          │
                                                  │                          │
                                                  └─ per-endpoint ArrayQueue ┘
                                                     + Notify (drop-oldest)
```

Lifecycle:
- **Startup:** `main` parses CLI, merges TOML config (TOML endpoints first, CLI appended; CLI globals override TOML globals; duplicate `#name` is a fatal error), builds an `EndpointSpec` for each requested endpoint, then spawns the router task followed by one reader + one writer task per endpoint. The msgid table is `const` — populated entirely at build time, no runtime construction. A shared `CancellationToken` is handed to every task. The spawner constructs each top-level endpoint's `EndpointId`, `Arc<EndpointStats>`, `TxQueue`, and `IdentityFlags` *before* the endpoint task is spawned, and announces them on the lifecycle channel as `EndpointEvent::EndpointAdded` so the router sees every routing endpoint before the first `RouterFrame` references it. Sub-endpoints (`tcps:` children, `udps:` peers) follow the same ordering via `PeerAdded`. **Bind/open failures at startup are not fatal**: each endpoint enters the same reconnect/retry loop it uses at runtime (TCP/UDP server bind reuses the `tcpc:` backoff curve; serial reuses `serial_reopen_ms`), so the router comes up immediately and endpoints attach as their resources become available. Each failed attempt logs at WARN.
- **Runtime:** all data flow is async, across four task roles: per-endpoint readers, per-endpoint writers, one router task, and one stats task. **Reader → router** is a single shared `tokio::sync::mpsc<(EndpointId, Bytes, ParsedHeader)>` of `ingress_queue_frames` capacity (default 1024) — senders **await on full**, i.e. backpressure on readers when the router is briefly behind. No silent ingress drops: the router task is doing a few dozen ns per frame (dedup, learn, per-destination decision — *not* stats snapshotting; see Stats sink architecture), so if it ever fills 1024 entries the architecture has a real problem and we want it visible as stalled readers, not invented as a counter. The reader parses the MAVLink header at frame-completion time while those bytes are still cache-hot, so the router never re-parses. **Router → writer** is one queue per endpoint, implemented as `Arc<crossbeam_queue::ArrayQueue<Bytes>>` (lock-free, bounded, default `tx_queue_frames = 256`) paired with `Arc<tokio::sync::Notify>` for wakeups. The router enqueues with `force_push(b)` which atomically inserts and returns the displaced `Bytes` on overflow; that return value increments per-endpoint `dropped_tx` and is dropped. Drop-oldest is the steady-state expectation for a slow consumer. Each task is a `loop { tokio::select! { ... } }` over its inputs and the cancellation token. **The router task owns the learn registry, the global dedup window, and per-destination decision state — sole writer for those.** The **stats task** owns the `Arc<EndpointStats>` registry mirror (fed by the router via `stats_event_tx`), the interval timer, and the stdout sink. Per-endpoint stats live in `Arc<EndpointStats>`, a struct of atomics shared with the reader, writer, router, and stats task; each counter is incremented by whichever task physically observes the counted event (`rx_*` in the reader, `tx_*` in the writer, `dropped_tx` wherever a `force_push` evicts or a `drain_and_discard` runs, `state` under the split-authority rule — see the EndpointState locked decision). Readers hold this `Arc<EndpointStats>`, a clone of the endpoint's `IdentityFlags`, an immutable `&'static` reference to the const msgid table, and per-endpoint config; writers hold the `Arc<EndpointStats>` and the `TxQueue`.
- **Shutdown:** Ctrl-C / SIGTERM trips the cancellation token. Each task is given up to 2s to drain its current operation (final TX flush, last frame write). After a 5s overall wall-clock budget, any remaining tasks are `abort()`ed via `JoinSet`; missed deadlines log at WARN.

### Endpoint types (target set)

| Scheme       | Description                          | Notes                                          |
|--------------|--------------------------------------|------------------------------------------------|
| `serial:`    | UART, form `path:baud` or `path,baud` | Hardware flow control, hot-replug recovery |
| `udps:`      | UDP server (bind, learn peers)       | Multiple peers per socket; idle-reap peers. Dual-stack on `[::]` (forced `IPV6_V6ONLY=0` on Windows + Linux for parity). `SO_REUSEADDR` on (no TIME_WAIT delay on restart); `SO_REUSEPORT` off. |
| `udpc:`      | UDP client (initial send to configured remote, then latch onto reply source) | First sends go to the configured `host:port`. On the first inbound packet **whose source IP matches the resolved configured host** (port may differ — handles ephemeral-port GCSes), destination is updated to that packet's `(ip, port)`. Packets from any other source IP are dropped at the socket and counted. If the latched peer is silent for `?latch_idle_secs=N` (default 30s), the router re-resolves DNS and reverts to the configured `host:port`. |
| `tcps:`      | TCP server (listen, accept many)     | Each accepted client is its own logical endpoint (own learn-set, own stats). `SO_KEEPALIVE` on (OS defaults). Dual-stack on `[::]`. `SO_REUSEADDR` on (no TIME_WAIT delay on restart); `SO_REUSEPORT` off. |
| `tcpc:`      | TCP client (dial, reconnect)         | Capped-exponential backoff 250 ms → 30 s ±20% jitter, `TCP_NODELAY`, `SO_KEEPALIVE` on (OS defaults). |

Out of scope for v1: UDP broadcast, WebSocket, MQTT, Unix domain sockets, multicast.

> **Note on TCP keepalive.** OS defaults are used (no custom idle/interval/probes). On Linux that means ~2 hours of idle before the first probe, so dead-peer detection over `tcps:`/`tcpc:` is **not aggressive**. If a deployment needs faster detection, set the kernel-wide `tcp_keepalive_*` sysctls or revisit this decision and wire custom params through `socket2`.

**Routing granularity for sub-connections.** A `tcps:` listener and a `udps:` socket each represent one *configured* endpoint, but routing operates at finer granularity:

- Each TCP client accepted on a `tcps:` is its own routing endpoint (independent learn-set, loop-prevention scope, stats). When it disconnects, the routing endpoint is torn down; the parent listener stays up.
- Each peer learned on a `udps:` (by source address) is its own routing endpoint, idle-reaped per `?idle_secs=` (default 60s). A new peer = a new routing endpoint.
- Filters declared on the listener apply to every child uniformly. Children inherit filters **and group membership** by reference, not by copy — adding a `tcps:` listener to `group=uplink` puts every accepted client in that group's shared learn-set.
- **Sub-endpoint naming.** Children inherit the parent's `#name` and append `/ip-port` from the peer's source address. A `tcps:0.0.0.0:5760#gcs` accepting `127.0.0.1:54321` produces routing endpoint name `gcs/127.0.0.1-54321`; a `udps:0.0.0.0:14550#bus` learning `192.168.1.10:14550` produces `bus/192.168.1.10-14550`. IPv6 source addresses appear bracketed (`bus/[2001:db8::1]-14550`). The name appears verbatim in stats output and `tracing` span fields. Names are not stable across reconnects — a re-accepting client gets a fresh name reflecting its new ephemeral port.

### Frame pipeline

```
  read raw bytes  →  framer (v1/v2 state machine)  →  routing decision  →  per-endpoint TX queue  →  write
                          ↓                              ↓
                    CRC check (known msgids)       learned (sysid,compid) table
                    extract target fields          per-endpoint filters
                                                   source suppression
                                                   dedup (optional)
```

The framer is a small state machine that reads into a per-endpoint `BytesMut` using the standard `tokio_util` codec pattern: bytes are read into the trailing capacity, and on each complete frame the framer calls `split_to(frame_len).freeze()` to produce an immutable `Bytes` and `reserve(n)` to maintain headroom for the next read. The underlying allocation is reclaimed once every writer has dropped its `Bytes` clone of the frame. At the same step the framer parses the MAVLink header (cheap, ~24 bytes of immediate-by-value fields) and emits `(EndpointId, Bytes, ParsedHeader)` on the router channel — the router never re-parses. From that point on the frame body is reference-counted and each writer receives a cheap `Bytes::clone` (Arc bump, no copy). v1 (`0xFE`) and v2 (`0xFD`) STX bytes are recognised; v2 IFLAG_SIGNED extends expected length by 13 bytes; v2 zero-trim is preserved (payload length comes from the header, not the payload tail). The signature trailer, when present, is part of the `Bytes` and is forwarded byte-for-byte without inspection — CRC validation only spans `header.len_byte..=payload_end` plus `crc_extra`, exactly as for unsigned frames.

Resync: when bytes don't match an STX or a frame fails length/CRC, the framer advances one byte and rescans. There is no upper bound on garbage-prefix length; a `resync_bytes` counter is incremented per skipped byte and exposed in stats.

Target offset bounds: for known msgids that carry `target_system`/`target_component`, the offset is checked against `payload_len`. If the offset lies *beyond* the (possibly zero-trimmed) payload, the target byte is implicitly `0` — i.e. broadcast. This is the correct behaviour for v2 zero-trim and must not crash the router.

### Routing

Broadcast-with-filter, identical in spirit to `mavlink-router`. The pipeline has two filter stages — one at ingress (source endpoint), one at egress (destination endpoint) — wrapped around a per-frame routing decision.

**Ingress pipeline (applied once per incoming frame; steps 1–3 run in the source endpoint's reader task before `frame_tx.send`, steps 4–5 run in the router task after `frame_rx.recv`):**

1. **CRC validation — reader task.** For known msgids; see MAVLink handling below. Bad CRC → drop, increment per-source-endpoint `crc_errors`.
2. **Seq-loss accounting — reader task.** For this frame's `(srcsys, srccomp)`, compute `gap = (seq - last_seq - 1) mod 256`. If `0 < gap < threshold` (default 64), add `gap` to the endpoint's `rx_lost_est`. If `gap >= threshold`, treat as a source restart / long silence: do not increment the counter, just reset `last_seq`. Update `last_seq = seq` either way. Runs before In-filter so the counter reflects link quality, not policy. The seq tracker state lives on the reader (cache-hot path; the `ParsedHeader` the reader already produced supplies `seq`).
3. **In-filter — reader task.** Apply this endpoint's `AllowMsgIdIn` / `BlockMsgIdIn` / `AllowSrcSysIn` / `BlockSrcSysIn` / `AllowSrcCompIn` / `BlockSrcCompIn` lists from the reader's `IdentityFlags` clone. Rejected frames bump `in_filter_drops` and are dropped — they never reach the router. Use to muzzle a chatty endpoint at the door.
4. **Deduplication — router task** (optional, `DeDuplicationPeriod` > 0). **Single global window** owned by the router (see the dedup locked decision); a frame's xxh3-64 hits the same window regardless of which source endpoint produced it. Lookup is O(1) via a `HashSet<u64>` of recent hashes; eviction order is driven by a parallel fixed-capacity FIFO ring of `(u64 hash, Instant deadline)` (default capacity 4096) that handles both TTL expiry (drop from the ring head while the front is past its deadline) and oldest-first eviction when the ring is full. On insert: expire stale entries from the head, then if the ring is at capacity evict the oldest, then push. If a live entry matches, drop the frame and bump `dedup_drops` on the source endpoint. The global scope is what makes the redundant-uplink use case work — LTE + RFD900 each deliver the same vehicle frame, the second arrival is suppressed.
5. **Learn — router task.** Add `(srcsys, srccomp)` to the source endpoint's *effective* learned set with an updated last-seen timestamp — per-endpoint by default, or shared across all members if the source endpoint has `?group=` set (see the Endpoint-groups locked decision). When the effective set is at `learn_capacity`, evict the entry with the oldest last-seen (LRU). The reader-side seq tracker uses an independent LRU at `seq_tracker_capacity`; the two structures share no state.

**Per-destination decision (run for every other endpoint):**

1. **Loop prevention.** Reject if the frame's `(srcsys, srccomp)` is in this endpoint's learned set. (Endpoints in the same `Group` share a learned set, so redundant parallel links do not silence each other. Group members share *only* the learn-set; filters and stats remain per-endpoint.)
2. **Sniffer override.** If this endpoint is flagged as a sniffer, **skip all remaining steps and accept**. A sniffer sees every frame the router has accepted, regardless of target, loop-prevention, or out-filters — this is the diagnostic/logging tap behavior from mavlink-router. Because ingress dedup runs *before* the per-destination decision (see the sniffer + dedup ordering locked decision), a sniffer sees the post-dedup frame set, not raw pre-dedup traffic. (Trade-off: a sniffer on a slow link can blow its own TX queue. Drop-oldest still applies.)
3. **Out-filter.** Apply this endpoint's `AllowMsgIdOut` / `BlockMsgIdOut` / `AllowSrcSysOut` / `BlockSrcSysOut` / `AllowSrcCompOut` / `BlockSrcCompOut` lists. Use to protect a specific consumer (e.g. an SBC that doesn't care about RC packets).
4. **Target match.** Accept if the frame is broadcast (`target_system` is 0, or msgid has no target field), or `(target_sys, target_comp)` is in this endpoint's learned set, or `target_comp` is 0 and `target_sys` is in the set.

Endpoints learn `(sysid, compid)` from every valid inbound frame. The learned set is a small fixed-capacity flat structure, not a `HashMap`.

**Filter semantics:**
- `Allow*` and `Block*` are *lists*, not single values. Empty `Allow*` means "no allow-restriction" (allow all). Non-empty `Allow*` means "only these pass." Non-empty `Block*` rejects matching items. If both are set, `Block*` wins on overlap.
- Filter scope: `*Out` filters are evaluated per destination; `*In` filters are evaluated once at ingress. Per-direction filters compose (a frame must pass the source's `*In` AND each destination's `*Out`).
- Filter cost: each `Allow*`/`Block*` list is small (typically <20 entries) and stored as the spec parser produces it — `Vec<MsgIdRange>` for the msgid axes, `Vec<U8Range>` for the src_sys and src_comp axes. Membership for a single value `x` is a linear scan testing `lo <= x <= hi` on each range. At n ≤ 20 a branch-predictable linear scan over contiguous ranges is faster than binary search (better cache behaviour, no pivot mispredicts) and avoids the need to keep the list sorted on insert. Filter lists allocate once at spec-parse time; the allocation is amortised against process lifetime and never touched on the per-frame hot path.

### Concurrency model

Async/await on **Tokio**, multi-threaded scheduler (default worker count). One task per endpoint (read), one task per endpoint (write), and a central router that owns the routing table and a broadcast/MPSC fan-out. Rationale:

- Tokio is the only async runtime with mature cross-platform serial + TCP + UDP support.
- Task-per-endpoint isolates a slow consumer from blocking others (with bounded TX queues that drop oldest on overrun, logged).
- The router task is the single owner of mutable routing state — no shared `Mutex` on the hot path.

Reject the temptation to put the routing decision inside the reader task: that distributes mutable state and complicates filters/dedup. Keep the router central.

**Reader backpressure has different consequences per transport.** The shared reader→router mpsc applies await-on-full backpressure to *every* reader uniformly, but what that backpressure means at the wire depends on the transport:

- **TCP (`tcps:` children, `tcpc:`):** a blocked reader stops draining the kernel receive buffer, which fills, which triggers TCP zero-window flow control back to the peer. The peer pauses. No data is lost; throughput is the only thing that suffers.
- **UDP (`udps:`, `udpc:`):** a blocked reader stops draining the socket. The kernel UDP receive buffer (`SO_RCVBUF`, default ~200 KB on Linux) fills, then **the kernel silently drops further datagrams**. This is invisible to RMR: the seq-gap-based `rx_lost_est` counter will catch *some* of it but cannot distinguish kernel-side drops from link-level loss. Operators relying on `udps:` or `udpc:` under sustained router stalls should size the OS UDP buffer (`net.core.rmem_max` / `SO_RCVBUF`) or accept the loss.
- **Serial (`serial:`):** a blocked reader stops calling `read()` on the file descriptor. The kernel tty/usbserial buffer fills (typically 4–16 KB), then **UART FIFO overruns drop bytes**. The framer's resync logic recovers, but mid-frame corruption is observed as `crc_errors` (for known msgids) or as `resync_bytes` spikes.

In short: router stalls are designed to be **rare** (bounded mpsc of 1024, ~50ns work per frame, single-threaded router task with no per-frame I/O — the only `await`s it makes are on lifecycle events: forwarding `StatsEvent` to the stats task on `EndpointAdded`/`PeerAdded`/`PeerRemoved`, and these fire orders of magnitude less often than frames), but when they happen, UDP and serial readers leak data the router can't see. The `rx_lost_est` counter is the user-visible signal that this might be occurring; an operator chasing it should also inspect kernel-level drop counters (`netstat -su` for UDP, `dmesg | grep overrun` for serial).

### MAVLink handling depth

The router uses a single in-memory table of `(msgid → {name, crc_extra, min_payload_len, target_sys_offset?, target_comp_offset?})`. `name` is the MAVLink message name as `&'static str` — purely a label for error messages, conflict diagnostics during the build-time merge, and `tracing` output; the router never matches on it. Populated in two layers:

1. **Compile-time built-in** — `build.rs` parses vendored MAVLink XML (default: `common.xml` + `ardupilotmega.xml`) with `quick-xml` (build-dependency) and emits `const SORTED: &[(u32, MsgEntry)]`, sorted by msgid. Lookup is binary search (≈9 comparisons for ~500 entries). Zero runtime cost, ~95% coverage. `<include>foo.xml</include>` is resolved recursively at build time, search path = the including file's directory, with cycle detection (fatal if detected).
2. **Pass-through** — unknown msgids forward as broadcast with no CRC check.

There is intentionally no runtime `--dialect` flag. Integrators with custom or proprietary dialects fork the repo, drop their XML under `vendor/mavlink/` (or a sibling directory), append the filename to the `DIALECTS` list in `build.rs`, and rebuild. The same `<include>` resolver, conflict checker (hard error on `crc_extra` mismatch with a built-in entry), and `crc_extra` computation apply to fork-added dialects automatically. Shipping an XML parser, include resolver, and conflict checker in the binary buys very little over pass-through — unknown msgids already forward correctly — and the target audience (integrators building from source) loses nothing by rebuilding.

`crc_extra` is *computed*, never read — MAVLink XML does not embed it. A helper (`crc_extra_for_message`) implements the standard algorithm: CRC-16-MCRF4XX over `msg_name` followed by each non-extension field's `type` and `name` (and array length for arrays), with fields visited in size-sorted order. The algorithm lives in `src/mavlink/crc_extra.rs` as **the single source of truth** and is `include!`d from `build.rs` (Cargo forbids `build.rs` from `use`ing items in the crate it builds). The runtime crate also exposes the function as `pub(crate) fn crc_extra_for_message(...)` so unit tests can assert it produces the expected `crc_extra` values for known messages (e.g. `HEARTBEAT` = 50, `SYS_STATUS` = 124) against published MAVLink reference data. Computed once per message at build time and baked into the const table.

For frames whose msgid is in the table, CRC is validated against `crc_extra` and the frame is rejected on mismatch (counted in stats). For unknown msgids, the frame forwards unconditionally — being dialect-agnostic is more important than being CRC-paranoid for a router.

Signed v2 frames: the 13-byte signature trailer is included in the frame slice but never inspected. CRC validation spans the same range as for unsigned frames (`header.len_byte..=payload_end` plus `crc_extra`); the signature trailer is forwarded byte-for-byte.

## Tech stack

| Concern        | Choice                            | Why                                                 |
|----------------|-----------------------------------|-----------------------------------------------------|
| Async runtime  | `tokio` (full)                    | Cross-platform, mature, async serial/TCP/UDP        |
| Serial         | `tokio-serial` (wraps `serialport-rs`) — added in Phase 4 | Windows COMx + Linux ttyS/ttyUSB, async-ready  |
| Sockets        | `socket2`                         | Set `IPV6_V6ONLY=0` for dual-stack `[::]` binds on Windows; future-proofs custom keepalive params if we tighten dead-peer detection later. |
| CLI            | `clap` (derive)                   | Standard                                            |
| Config         | `serde` + `toml`                  | Idiomatic Rust; INI (mavlink-router's choice) is not |
| JSON output    | `serde_json`                      | Stats JSON-Lines on stdout. Not used for any internal wire format. |
| Timestamps     | `time` (with `formatting` + `macros` features) | ISO 8601 / RFC 3339 timestamps in stats output. Lighter than `chrono`. Build-time only via `tracing-subscriber`'s default `time` integration for log timestamps too. |
| Logging        | `tracing` + `tracing-subscriber`  | Structured, level-filtered, async-friendly          |
| Errors         | `thiserror` only                  | Per-module error enums, joined at the binary boundary by a top-level `Error` enum with `From` impls. No `anyhow`. |
| MAVLink        | **Hand-rolled minimal framer.** Do **not** pull in `rust-mavlink` for routing — it codegens full message types we do not need. The `crc_extra` algorithm is implemented locally (small, well-defined). |
| Dialect XML    | `quick-xml` (`[build-dependencies]` only)                                          | Lightweight pull-parser used by `build.rs` to consume vendored MAVLink XML. Not a runtime dependency. |
| Buffers        | `bytes::Bytes` / `BytesMut`       | Reference-counted, zero-copy fan-out from router to writers |
| TX queues      | `crossbeam-queue::ArrayQueue` + `tokio::sync::Notify` | Lock-free bounded MPMC ring whose `force_push` atomically inserts and returns the displaced element on overflow — the exact drop-oldest semantics we need. `tokio::sync::mpsc` does not fit: its `Receiver` is single-consumer and owned by the writer task, so the sender side has no way to evict the head on `Full`. |
| Hashing        | `twox-hash` (xxh3-64)             | Fast non-cryptographic hash for dedup window |

Edition `2024`. **MSRV pinned at `1.85`** (declared via `rust-version` in `Cargo.toml` — the minimum Edition 2024 requires). Bump deliberately; do not raise without a reason that maps to a feature being adopted.

Adding new dependencies is fine as long as they are well-known and maintained. Avoid micro-crates and avoid bespoke string-manipulation/algorithm crates when the same logic is ~10 lines locally.

**Cargo features.** All transports (`serial:`, `udps:`, `udpc:`, `tcps:`, `tcpc:`) are always built. No Cargo features in v1.

## CLI shape (target)

```
rmr [GLOBAL OPTS] ENDPOINT [ENDPOINT ...]

ENDPOINT := SCHEME:SPEC[#name][?key=val&...]
  serial:/dev/ttyUSB0:921600
  serial:COM3:115200
  udps:0.0.0.0:14550
  udpc:192.168.1.5:14550
  tcps:0.0.0.0:5760
  tcpc:companion.local:5760#vehicle?group=uplink
  udps:0.0.0.0:14551#tap?sniffer=true
  tcpc:gcs.local:5760?block_msgid_in=33,32&allow_src_sys_out=1

GLOBAL OPTS:
  -c, --config FILE       TOML config (endpoints + globals)
      --log-level LEVEL   trace|debug|info|warn|error (default info)
      --log-format FMT    text (default) | json
      --stats             enable periodic per-endpoint stats (JSON-Lines on stdout)
      --stats-interval N  stats output interval in seconds (default 5)
      --dedup-ms N        duplicate suppression window (0 = off)
      --shutdown-grace N  overall wall-clock shutdown budget, seconds (default 5)
```

Output channels:
- **stdout** is reserved for stats (JSON-Lines, one object per period). Quiet when `--stats` is off.
- **stderr** carries logs (`tracing` output). Default format is human-readable; `--log-format=json` switches to structured JSON via `tracing_subscriber::fmt().json()`.

CLI + TOML merge: TOML endpoints are processed first, CLI endpoints appended in argv order. Endpoint names (`#name`) must be unique across the combined set — a duplicate is a fatal error, not a merge. CLI globals override TOML globals on a per-key basis.

Endpoint name defaults: when `#name` is omitted, a name is derived from `scheme-addr-port` (e.g. `udps:0.0.0.0:14550` → `udps-0_0_0_0-14550`) so logs and stats never display `endpoint=<unnamed>`. Characters outside `[A-Za-z0-9_-]` in the addr part (dots in IPv4, colons in IPv6, slashes in serial paths) are replaced with `_` so the auto-name satisfies the same regex as explicit names. Explicit `#name` values must match `[A-Za-z0-9_-]{1,64}` — anything else is a parse-time error with a clear message (so names are safe in logs, JSON stats, span fields, and as sub-endpoint name prefixes).

**Filter list grammar.** All `allow_*` / `block_*` query values are comma-separated lists of **decimal integers and decimal `lo-hi` ranges**, e.g. `block_msgid_in=33,100-150,32`. Whitespace around items is tolerated; everything else (hex literals, symbolic names like `HEARTBEAT`, wildcards) is a parse-time error with a clear message. Ranges are inclusive on both ends; `lo > hi` is fatal. Same grammar applies in TOML (`block_msgid_in = "33,100-150,32"` as a string, parsed identically). **TOML accepts the string form only** — array forms like `block_msgid_in = [33, "100-150", 32]` are a parse-time error. Rationale: one parser for both CLI and TOML; the array form would also force the type system to accept mixed-typed array elements (integer + string) since the range syntax requires a string anyway.

Startup validation is strict for *configuration*: unknown query-string keys are fatal with a "did you mean" suggestion, malformed addresses are fatal, and duplicate `#name`s are fatal. Duplicate endpoint addresses (same scheme + bind/dial target) log at WARN but do not abort. *Runtime* resources (sockets, devices) are treated as may-be-unavailable: bind/open failure at startup is not fatal — the affected endpoint enters its normal reconnect/retry loop and the rest of the router comes up. Each failed attempt logs at WARN so misconfiguration is still visible.

### Defaults

All per-endpoint values are overridable via `?key=val` on the endpoint string or by the matching TOML key.

| Knob                    | Default     | Notes |
|-------------------------|-------------|-------|
| `read_buf_bytes`        | 8192        | Per-endpoint `BytesMut` initial capacity (reserved after each frame freeze) |
| `tx_queue_frames`       | 256         | Per-endpoint writer `ArrayQueue` depth; drop-oldest via `force_push` on overflow |
| `ingress_queue_frames`  | 1024        | Shared reader→router channel depth; senders await on full (backpressure, not drop) |
| `event_queue_size`      | sized at spawn | Shared lifecycle channel (`EndpointEvent`) depth; sized at spawner-construction time using the same formula as `stats_event_tx` — `max(64, 2 × N)` where `N = top-level-endpoint-count + sum(udps_peer_capacity) + sum(tcps_peer_budget)` (`tcps_peer_budget` default 64). Senders await on full; lifecycle events are rare relative to frames so brief blocking is acceptable. |
| `learn_capacity`        | 32          | `(sysid, compid)` entries per endpoint. Parsed by the spec layer today; consumed by the router learn-table in Phase 5. Setting it on the CLI/TOML before Phase 5 lands is silently accepted (no WARN) but has no runtime effect. |
| `seq_tracker_capacity`  | 32          | `(sysid, compid) → last_seq` entries per endpoint. Same Phase 5 wiring status as `learn_capacity`. |
| `dedup_window_capacity` | 4096        | Total `(hash, deadline)` entries — single window owned by the router (global, not per-endpoint) |
| `dedup_ms`              | 0 (off)     | Dedup TTL; >0 enables the window |
| `idle_secs` (`udps:`)   | 60          | Peer expiry on inactivity (query key: `?idle_secs=N` on a `udps:` endpoint) |
| `udps_peer_capacity`    | 256         | Per-listener cap on simultaneously-tracked peers; LRU eviction by last-seen when full |
| `latch_idle_secs`       | 30          | `udpc:` peer silence threshold; on expiry, revert destination to configured `host:port` and re-resolve DNS |
| `reconnect_initial_ms`  | 250         | `tcpc:` backoff floor |
| `reconnect_max_ms`      | 30000       | `tcpc:` backoff ceiling (+ ±20% jitter) |
| `serial_reopen_ms`      | 1000        | Hot-replug poll interval |
| `shutdown_grace_secs`   | 5           | Overall wall-clock budget on shutdown |
| `stats_interval_secs`   | 5           | Period of stats JSON-Lines output |
| `stats_queue_lines`     | 256         | Stats-task bounded queue depth; drop-oldest on slow/dead stdout consumer (single counter exposed at WARN) |
| Seq-gap sanity threshold | 64         | Above this, treat as source restart, not loss |

### Stats schema (JSON-Lines on stdout)

One object per period, per endpoint:

```json
{"ts":"2026-05-15T19:00:00Z","endpoint":"vehicle","state":"connected",
 "rx_frames":12450,"tx_frames":12440,"rx_bytes":2891200,"tx_bytes":2889600,
 "dropped_tx":0,"crc_errors":2,"resync_bytes":7,"rx_lost_est":3,
 "in_filter_drops":0,"out_filter_drops":1,"dedup_drops":15,"learn_entries":4}
```

Counters are cumulative-since-start. Consumers compute deltas.

`state` is one of `connected | reconnecting | idle | down`:
- `connected` — transport open and exchanging frames (or ready to); for `udps:` learned peers, written by the parent listener on admission since UDP has no transport-up event.
- `reconnecting` — backoff loop is running, either after a disconnect or as the **initial state at process startup** before the first successful connect/bind/open.
- `idle` — `udps:` learned-peer that has not been seen for `idle_secs` but has not yet been reaped; included in stats output for one final period so consumers can see the transition.
- `down` — terminal state for a `tcps:` child after its socket closed, emitted once before the routing endpoint is removed.

## Roadmap

Each phase ends in a usable binary. Don't skip ahead; each phase exposes integration bugs the next phase relies on being absent.

**Caveat for Phases 0–3:** the endpoint modules and their integration tests work today by calling each `run(spec, wiring)` directly. `lib.rs::run` itself parses specs and idles — the binary becomes operational only once Phase 5 lands the top-level spawner. Phase 0–3 `[x]` marks therefore mean "endpoint code is implemented, tested in-tree, and ready to be wired up," not "the `rmr` binary routes traffic." Treat this as a one-off carve-out tied to the spawner deferral; later phases are expected to deliver a fully working binary at completion.

### Phase 0 — skeleton (current state + bootstrap)

- [x] `Cargo.toml` with `tokio`, `tokio-util` (CancellationToken), `clap`, `tracing`, `tracing-subscriber` (with `json` feature), `serde`, `serde_json`, `toml`, `thiserror`, `bytes`, `twox-hash`, `socket2`, `crossbeam-queue`, `time` (with `formatting` + `macros` features), `proptest` (dev). `quick-xml` is a `[build-dependencies]` entry only — not a runtime dependency. `rust-version = "1.85"` set in `[package]`.
- [x] CLI parsing (clap derive), `--log-format text|json`, log init, graceful Ctrl-C shutdown via `CancellationToken` with a 5s overall `JoinSet`-abort fallback (the documented per-task 2s drain refactor is deferred to Phase 5, where the top-level spawner actually puts endpoint tasks into the `JoinSet`)
- [x] `EndpointSpec` enum + parser for scheme-prefixed strings — full unit test coverage of valid + invalid forms, plus strict-validation rejection of unknown `?key=` names with a "did you mean" suggestion, `#name` validated against `[A-Za-z0-9_-]{1,64}`, plus auto-naming when `#name` is omitted
- [x] Repo layout scaffolded per the Project layout section: empty modules under `src/{mavlink,endpoint,router}/`, `src/lib.rs` re-exporting them, empty `tests/` directory committed with `.gitkeep`, `fixtures/` directory committed
- [x] CI workflow (GitHub Actions) runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-features` on every push — all three must pass. Lint discipline is enforced from day one, not retrofitted.
- [x] `cargo test` passes (skeleton tests only); `cargo build` produces a binary that prints CLI help when invoked with `--help`

### Phase 1 — frame layer

- [x] v1/v2 framer with resync (`resync_bytes` counter), zero-trim handling, `BytesMut` accumulator with `split_to(frame_len).freeze()` + `reserve(n)` on each complete frame; parsed `ParsedHeader` returned alongside the frozen `Bytes`
- [x] `crc_extra_for_message` helper (size-sorted, extension-aware) lives in `src/mavlink/crc_extra.rs`; `build.rs` consumes it via `include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mavlink/crc_extra.rs"))`; runtime tests assert known `(msg_name → crc_extra)` pairs match published MAVLink reference values
- [x] `build.rs` consuming the `DIALECTS` list (defaults to `common.xml` + `ardupilotmega.xml`) with recursive `<include>` resolution against the including file's directory, cycle detection (fatal), and fatal error on `crc_extra` conflict between dialects → emit `const SORTED: &[(u32, MsgEntry)]` sorted by msgid (binary-search lookup)
- [x] CRC validation against `crc_extra` for known msgids; pass-through for unknown
- [x] Target-offset bounds check against `payload_len` (zero-trim → implicit 0 = broadcast)
- [x] Unit tests: known-good capture replay → expected frames out; signed v2 frame validates CRC and forwards signature trailer opaquely
- [x] Property test: garbage in → no panics, no infinite loops, framer always resyncs within bounded bytes
- [x] Integration test: a frame with a known msgid gets CRC-validated and targeted-routed; a frame with an unknown msgid forwards as broadcast without CRC check

### Phase 2 — UDP endpoints

- [x] `udps:` — bind, multi-peer learning, idle reap (configurable, default 60s); dual-stack on `[::]` via `socket2` setting `IPV6_V6ONLY=0` on both Windows and Linux for parity
- [x] `udpc:` — send to configured `host:port` initially; latch onto inbound source address only when the source IP matches the resolved configured host (port may differ); drop + count packets from other IPs at ingress; revert to configured address after `latch_idle_secs` (default 30s) of silence from the latched peer, re-resolving DNS on revert; DNS re-resolve on every send burst after a `send_to` error
- [x] Round-trip integration test: two `udps:` instances exchange frames
- [x] `udpc:` integration test: configured peer sends from a different ephemeral port (same IP) → router latches and replies to the latched addr; packet from an unrelated IP is dropped + counted; after `latch_idle_secs` of silence the router reverts to the configured `host:port`

### Phase 3 — TCP endpoints

- [x] `tcps:` — accept loop, per-client task, **each accepted client is its own routing endpoint** (own learn-set, own stats, filter inheritance by reference from the listener); `SO_KEEPALIVE` on (OS defaults), `TCP_NODELAY`, dual-stack on `[::]` via `socket2` (`IPV6_V6ONLY=0`); removal on disconnect without killing the router (`mavlink-router` had this bug)
- [x] `tcpc:` — dial, capped-exponential backoff (250 ms → 30 s, ±20% jitter), `TCP_NODELAY`, `SO_KEEPALIVE` on (OS defaults), DNS re-resolve on each reconnect
- [x] Initial bind/dial failure path: `tcps:` that can't bind enters the same backoff loop as `tcpc:`; router stays up and logs at WARN per attempt
- [x] Integration test: kill a TCP client mid-stream, verify router stays up and reconnects within the backoff bound
- [x] Integration test: start a `tcps:` on a port already in use, free the port, verify the listener picks it up without restart

### Phase 4 — serial

- [x] **`tokio-serial` dep + `SerialSpec`/`SerialWiring` contract.** Add `tokio-serial` to `[dependencies]` (version pin chosen at commit time). Define the scheme-typed `SerialSpec`/`SerialWiring` pair following the same shape as `tcp::client` and `udp::client` — `SerialSpec` carries the device path, baud, optional RTS/CTS, `IdentityFlags`, parent `EndpointId`, name, and `serial_reopen_ms`; `SerialWiring` carries the `frame_tx`, `tx_queue`, `stats`, and cancellation token. No `event_tx` (serial has no children). No `bound_addr_tx` (no socket to bind). RTS/CTS query-key name locked as `?flow_control=rtscts|none`, defaulting to `none` (rejected: `hw`, ambiguous against DTR/DSR; rejected: `?rtscts=true`, doesn't leave room for adding DTR/DSR later without a second key).
- [x] `serial:` open, baud setting, optional RTS/CTS
- [x] Hot-replug recovery: on read error or device removal, periodically attempt re-open (mavlink-router does not do this; we should)

### Phase 5 — routing

- [ ] **Top-level spawner.** `lib.rs::run` builds the shared reader→router `frame_tx` mpsc (`ingress_queue_frames` capacity) and the lifecycle `event_tx` mpsc, spawns the router task, then matches each parsed `EndpointKind` to its scheme-typed `*Spec`/`*Wiring` entry point (`udp::server::run(UdpServerSpec, UdpServerWiring)`, `tcp::client::run(TcpClientSpec, TcpClientWiring)`, …). Top-level `EndpointId`s are allocated by the spawner (not by each endpoint task); per-endpoint `Arc<EndpointStats>`, `TxQueue`, and `IdentityFlags` are constructed by the spawner and announced to the router via `EndpointEvent::EndpointAdded`. The spawner `await`s `event_tx.send(EndpointAdded)` to completion *before* spawning the endpoint task that will hold the matching `EndpointId`; combined with the router's biased select over `event_rx` then `frame_rx` (locked in the symmetric-registration decision below), this makes the "router knows about every endpoint before its first frame" invariant enforceable through the existing two-channel design. `EndpointAdded` fires at spawn time regardless of bind/dial outcome — a top-level endpoint whose port is held by another process registers in state `Reconnecting` and is visible in stats from the first interval. **Today's `lib.rs::run` parses specs and idles** — the Phase 0–3 endpoint modules and their integration tests work by calling each `run(spec, wiring)` directly; the binary itself becomes operational only when this bullet lands.
- [ ] **Per-task 2s shutdown drain.** Extend `shutdown.rs` from the current single 5s global budget to the documented two-tier deadline: each task in the top-level `JoinSet` gets up to 2s after the cancellation token trips to complete its current operation (final TX flush, final frame write, `PeerRemoved` emission); remaining tasks are `abort()`ed once the 5s wall-clock budget expires. **Granularity:** the budget applies per top-level `JoinSet` entry, *not* per leaf task. A `tcps:` listener with N accepted children shares its 2s budget across the parent's drain plus all N child sessions (children run on the listener's inner `JoinSet`); similarly a `udps:` listener shares its 2s across the listener loop and its per-peer writer tasks. Top-level endpoints that don't spawn children (`tcpc:`, `udpc:`, `serial:`) get a private 2s each. **The router task and the stats task are also top-level `JoinSet` entries and each gets its own private 2s.** The router uses it to drain `event_rx` for any in-flight `PeerRemoved`, write the final `Down` state for top-level endpoints, and fire `StatsEvent::Finalize` for each before dropping its registry. The stats task uses it to process any pending Finalizes from its `stats_event_rx` and emit the corresponding final synthetic stats lines via the bypass path (per-line 100ms timeout); genuinely-undeliverable lines log at WARN. Rationale: keeps the budget structure aligned with the spawner's own `JoinSet`, avoids the multiplicative drain budget that "2s per leaf" would imply for large `tcps:` fan-outs.
- [ ] **`EndpointEvent::EndpointAdded`** — symmetric to `PeerAdded` for top-level endpoints. Payload: `id`, `name`, `tx_queue`, `stats`, plus a clone of the endpoint's `IdentityFlags`. Sub-endpoints inherit their parent's `IdentityFlags` by clone at spawn time and the parent forwards them on `PeerAdded` — i.e. the existing `PeerAdded` variant gains an `identity: IdentityFlags` field as part of this phase. No `EndpointRemoved` variant for top-level endpoints in v1 — they live for the process; on shutdown the router writes `state = Down` for every registered top-level endpoint and emits one final synthetic stats line per endpoint within the drain budget.
- [ ] **`EndpointState` on `EndpointStats`.** A single `AtomicU8` slot named `state` on `EndpointStats`, stable discriminants `Connected = 0 | Reconnecting = 1 | Idle = 2 | Down = 3` (named `EndpointState` for the enum, stored as raw u8 so the stats task can `load → match` cheaply). **`Connected = 0` is intentional:** `EndpointStats::default()` therefore lands in state `Connected`, which is the right initial value for sub-endpoints (UDP peers, TCP accepted clients) whose admission *is* the transport-up event. Top-level endpoints that go through bind/dial backoff get an explicit `state.store(Reconnecting as u8, Relaxed)` from the spawner before the endpoint task starts. **Write authority is split:** the endpoint task owns `Connected`/`Reconnecting`; the router owns `Idle`/`Down` and writes them in the same step as it processes the corresponding `PeerRemoved` (or, for top-level endpoints, the cancel-token-triggered shutdown sweep), emits one final synthetic stats line, then drops the `Arc<EndpointStats>` from its registry. **Shutdown happens-before:** once an endpoint task observes the cancellation token, it must not write `state` again — the router's `Down` write is then guaranteed to be the last write. Interval snapshots that fire inside the drain window may show the pre-cancel state until the final synthetic line; operators treat the final synthetic line as authoritative. **UDP peers** never enter `Reconnecting` (no transport-up/down event exists); transitions are admission → `Connected` (via `default()`), idle-reap → `Idle`, listener-shutdown / LRU-eviction → `Down`. `Idle`/`Down` are therefore never observed on a regular interval-driven snapshot — only on lifecycle-final synthetic snapshots — which is what makes the existing `PeerRemovalReason` → stats `state` mapping (`Idle → final idle line then removal`) operationalisable.
- [ ] **In-filter evaluation in the reader task.** Doc-level decision says rejected frames never reach the router — so In-filters live in the endpoint reader, after framing and CRC, before `frame_tx.send`. The filter snapshot reaches the reader through its `*Spec` (not through `*Wiring`, because filters are per-identity, not per-plumbing). Out-filters, sniffer override, and target-match all stay in the router. **Counter overlap:** `in_filter_drops` is the union of all ingress-side drops — configured In-filter rejections and the existing `udpc:` wrong-source-IP drops share this counter. It's a single `AtomicU64` on `EndpointStats`; any task that physically drops a frame at ingress increments it (the reader task for configured filters, the `udpc:` client task for wrong-source-IP, etc.). Operators distinguishing the two consult DEBUG traces; the counter is not split because both signals mean "frames the endpoint refused to admit."
- [ ] Central router task with learned `(sysid, compid)` → endpoint set (fixed-capacity flat struct, cap 32 by default, LRU eviction by last-seen)
- [ ] Bounded per-endpoint TX queue: `Arc<crossbeam_queue::ArrayQueue<Bytes>>` + `Arc<tokio::sync::Notify>` (default capacity `tx_queue_frames = 256`). Router enqueues with `force_push(b)` (signature `fn force_push(&self, value: T) -> Option<T>` — `Some(prev)` carries the displaced element on overflow); on `Some(prev)` the `dropped_tx` counter increments and the displaced `Bytes` is dropped. After every push the router calls `notify.notify_one()`. The writer task drains in a loop: `while let Some(b) = q.pop() { write(b).await? }` then `notify.notified().await`. On disconnect / before reconnect the writer drains-and-discards the queue (incrementing `dropped_tx` by the drained count) so no stale frames are flushed on the fresh link.
- [ ] Source-suppression (loop prevention)
- [ ] Targeted vs broadcast decision based on msgid table
- [ ] **Per-endpoint In-filters** (ingress, applied at the source): `allow_msgid_in`, `block_msgid_in`, `allow_src_sys_in`, `block_src_sys_in`, `allow_src_comp_in`, `block_src_comp_in`
- [ ] **Per-endpoint Out-filters** (egress, applied per destination): `allow_msgid_out`, `block_msgid_out`, `allow_src_sys_out`, `block_src_sys_out`, `allow_src_comp_out`, `block_src_comp_out`
- [ ] **Sniffer mode** per endpoint (`sniffer = true`): bypasses loop-prevention, target-match, and out-filters; receives every accepted frame
- [ ] **Duplicate suppression window** (`DeDuplicationPeriod` ms, default 0 = off): xxh3-64 over the framed bytes, fixed-capacity ring (default 4096) with TTL eviction on insert, applied at ingress before learn
- [ ] Endpoint groups (implicit by `?group=name` / TOML `group=`; shared learned set only — filters and stats stay per-endpoint)
- [ ] Per-endpoint stats counters: `rx_frames`, `tx_frames`, `rx_bytes`, `tx_bytes`, `dropped_tx`, `crc_errors`, `in_filter_drops`, `out_filter_drops`, `dedup_drops`, `resync_bytes`, `rx_lost_est`, `learn_entries`, plus the `state` field
- [ ] Per-source seq tracker on each endpoint feeding `rx_lost_est` (cap 32 entries, LRU eviction by last-seen, gap-sanity threshold default 64; reset on restart-sized gaps)

### Phase 6 — config file

- [ ] TOML parser for the same surface as CLI plus filters and groups
- [ ] CLI + file merge rules implemented: TOML endpoints first then CLI endpoints (no per-key merging by name — name collision is a fatal error), CLI globals override TOML globals
- [ ] Stretch: SIGHUP reload on Unix, file-watcher reload on Windows

### Phase 7 — polish

- [ ] **Two-class priority TX queue per endpoint** (`low_priority_msgids` config list, weighted drain ratio configurable, default 4:1). High class is the existing single queue from Phase 5; low class is added alongside. No reordering within a class. The retrofit must not change `TxQueue`'s appearance in `EndpointEvent::EndpointAdded`/`PeerAdded` or in any `*Wiring` struct — internals switch to two `ArrayQueue<Bytes>` + weighted drain, the API gains `push_low(b)`, and existing `push(b)` keeps "high-class" semantics so Phase 5 callers are correct by default.
- [ ] Stats output: dedicated stats task draining a bounded `mpsc<StatsLine>` (`stats_queue_lines`, default 256); JSON-Lines on stdout, one object per `--stats-interval` (default 5s), enabled by `--stats`; `ts` field formatted by `time` crate as RFC 3339 UTC; `BrokenPipe` on stdout logs once at WARN and continues; overflow increments `stats_dropped` and emits a WARN at most once per interval. Schema documented in this file.
- [ ] Cross-platform CI (GitHub Actions: linux-x86_64, linux-aarch64, windows-x86_64, macos) — extends the ubuntu+windows matrix already in place from Phase 0
- [ ] Remove the temporary `[lints.rust] dead_code = "allow"` from `Cargo.toml` and delete any code that is genuinely unused
- [ ] Release artifacts (musl static for Linux, MSVC for Windows)

### Stretch (not v1)

- Optional `.tlog` recording per endpoint or globally
- Bandwidth shaping / rate limiting per endpoint
- Prometheus stats endpoint

## Reference projects

- **mavlink-router** (https://github.com/mavlink-router/mavlink-router) — closest to RMR in architecture. INI config, broadcast-with-filter, learned sysid table, endpoint groups, single-threaded epoll. Lacks Windows support and hot-replug for serial — both of which RMR fixes.
- **mavp2p** (https://github.com/bluenviron/mavp2p) — closest to RMR in distribution model (single binary, scheme-prefixed CLI args, Windows + Linux). Diverges by emitting heartbeats and stream requests; RMR does not.
- **MAVProxy** (https://github.com/ArduPilot/MAVProxy) — reference for what a GCS does and what RMR therefore refuses to do.

## Project layout

The repo is one binary crate. Module boundaries match the architecture diagram so a contributor can map the doc to the code without translation. **Add new functionality to the existing module that owns the concern** — do not create new top-level modules without a clear reason.

```
rmr/
├── Cargo.toml
├── CLAUDE.md                         ← this file
├── README.md                         ← user-facing intro (eventually)
├── build.rs                          ← parses vendored MAVLink XML, emits const msgid table
├── build_support/                    ← build-only Rust modules `include!`d by build.rs (and by tests/build_support.rs for unit-test coverage); not part of the runtime crate
├── vendor/mavlink/                   ← in-tree copy of mavlink/mavlink XML at a pinned release tag (common.xml, ardupilotmega.xml, transitive includes, UPSTREAM.md)
├── fixtures/                         ← test fixtures: capture binaries, sample dialect XMLs
├── src/
│   ├── main.rs                       ← binary entrypoint: argv → spawn tasks → wait
│   ├── lib.rs                        ← library entrypoint (re-exports for integration tests)
│   ├── cli.rs                        ← clap derive structs, EndpointSpec string parser
│   ├── config.rs                     ← TOML schema (serde), CLI+file merge rules
│   ├── error.rs                      ← top-level Error enum, From impls for module errors
│   ├── shutdown.rs                   ← CancellationToken plumbing, signal handling
│   ├── mavlink/
│   │   ├── mod.rs                    ← re-exports
│   │   ├── frame.rs                  ← Frame view, FrameHeader, version detection
│   │   ├── framer.rs                 ← v1/v2 state machine, resync, zero-trim
│   │   ├── crc.rs                    ← CRC-16-MCRF4XX (frame CRC)
│   │   ├── crc_extra.rs              ← `crc_extra_for_message` algorithm — single source of truth, `include!`d by build.rs and exposed as `pub(crate)` for runtime tests
│   │   ├── msgid_table.rs            ← MsgEntry type and lookup over the build-time const slice
│   │   └── generated.rs              ← `include!` from build.rs OUT_DIR
│   ├── endpoint/
│   │   ├── mod.rs                    ← `EndpointId` + `EndpointIdAllocator`, sub-endpoint naming, `wait_or_cancel` shared by every transport
│   │   ├── spec/                     ← typed endpoint-spec parser (CLI string → fully-typed `EndpointSpec`). Tests live in each file as `#[cfg(test)] mod tests`, next to the code they exercise.
│   │   │   ├── mod.rs                ← `EndpointSpec` and the `parse()` entry point
│   │   │   ├── endpoint_kinds.rs     ← `EndpointKind` + the five per-scheme `*Endpoint` structs, each carrying a `common: CommonQuery` (plumbing knobs: `read_buf_bytes`, `tx_queue_frames`) and an `identity: IdentityFlags` (filters, sniffer, group, learn/seq capacities)
│   │   │   ├── error.rs              ← `SpecError`
│   │   │   ├── parse.rs              ← body/address parsers, name validation, scheme dispatch
│   │   │   └── query.rs              ← query-string parser, per-scheme `QueryApplier`s, `CommonQuery::apply`, value parsers, did-you-mean suggestion (walks `COMMON_KEYS`, `IdentityFlags::KEYS`, `Filters::KEYS`, and the per-scheme `*_EXTRA` lists)
│   │   ├── filters.rs                ← `Filters` struct (12 `allow_*`/`block_*` lists), `MsgIdRange` / `U8Range`, range-list parsers. Phase 5 hangs `passes_in_filter` / `passes_out_filter` off `Filters`.
│   │   ├── identity_flags.rs        ← `IdentityFlags`: `filters: Filters` + `sniffer` + `group` + `learn_capacity` + `seq_tracker_capacity`. Travels with every `*Spec`; cloned onto each sub-endpoint at admission.
│   │   ├── events.rs                 ← `RouterFrame`, `EndpointEvent` (`EndpointAdded` / `PeerAdded` / `PeerRemoved`), `PeerRemovalReason`
│   │   ├── stats.rs                  ← `EndpointStats` (per-endpoint counters + `state: AtomicU8`), `EndpointState` enum, `FramerCounters` delta helper
│   │   ├── tx_queue.rs               ← bounded queue with drop-oldest (`force_push` + `pop_or_wait` + `drain_and_discard`)
│   │   ├── backoff.rs                ← capped-exponential `Backoff` with ±20% jitter + `bind_with_backoff` helper shared by `tcps:`, `udps:`, `udpc:`
│   │   ├── defaults.rs               ← cross-endpoint default constants (read buf, tx queue, reconnect curve)
│   │   ├── socket.rs                 ← `bind_tcp_dual_stack`, `bind_udp_dual_stack`, `configure_tcp_stream` (`IPV6_V6ONLY=0`, `SO_REUSEADDR`, `TCP_NODELAY`, keepalive); used by every IP transport
│   │   ├── session.rs                ← generic `run_session<S: AsyncRead + AsyncWrite>` + `SessionOutcome` shared by `serial:`, `tcpc:`, `tcps:` children
│   │   ├── serial.rs                 ← `serial:` open + hot-replug loop wrapping the shared session
│   │   ├── udp/
│   │   │   ├── mod.rs                ← submodule declarations
│   │   │   ├── server.rs             ← `udps:` bind, peer map, idle reap
│   │   │   └── client.rs             ← `udpc:` initial-remote + reply-source latching, re-resolve on send-fail
│   │   └── tcp/
│   │       ├── mod.rs                ← submodule declarations
│   │       ├── server.rs             ← `tcps:` accept loop, child endpoints
│   │       └── client.rs             ← `tcpc:` dial + backoff reconnect + DNS re-resolve
│   └── router/
│       ├── mod.rs                    ← Router task entrypoint, mpsc wiring
│       ├── learn.rs                  ← learned (sysid, compid) table per endpoint/group
│       ├── group.rs                  ← endpoint groups, shared learn-set semantics
│       ├── decide.rs                 ← per-destination decision (sniffer, target, out-filter)
│       └── dedup.rs                  ← frame-hash window with TTL eviction
├── tests/                            ← integration tests, transport-grouped under tcp/ and udp/ (each subfolder is one Cargo test binary via its `main.rs`); transport-agnostic tests stay at the top level
│   ├── common/                       ← shared fixtures (mavlink frame builders, spawn harnesses, shutdown helper); pulled into each test binary via `#[path = "../common/mod.rs"] mod common;`
│   ├── framer_replay.rs
│   ├── build_support.rs              ← integration test binary that `include!`s each file under `build_support/` inside its own `mod`, so their `#[cfg(test)] mod tests` blocks run under `cargo test`
│   ├── tcp/
│   │   ├── main.rs                   ← aggregator: `mod common; mod bind_retry; mod identity; mod reconnect; mod roundtrip;`
│   │   ├── roundtrip.rs
│   │   ├── bind_retry.rs
│   │   ├── identity.rs               ← asserts every `tcps:` accepted child inherits a clone of the parent listener's `IdentityFlags` via `PeerAdded`
│   │   └── reconnect.rs
│   ├── udp/
│   │   ├── main.rs                   ← aggregator: `mod common; mod bind_retry; mod identity; mod idle_reap; mod latch; mod roundtrip;`
│   │   ├── roundtrip.rs
│   │   ├── bind_retry.rs
│   │   ├── identity.rs               ← asserts every `udps:` learned peer inherits a clone of the parent listener's `IdentityFlags` via `PeerAdded`
│   │   ├── latch.rs
│   │   └── idle_reap.rs
│   ├── serial.rs                     ← PTY-pair session coverage (Unix only via `#[cfg(unix)]`)
│   ├── filters.rs
│   ├── sniffer.rs
│   └── dedup.rs
└── benches/                          ← criterion benchmarks (Phase 7+)
    ├── framer.rs
    └── routing.rs
```

**Why a `lib.rs`?** Exposing the crate as a library lets integration tests in `tests/` and benches in `benches/` import internals without going through the binary. The `main.rs` becomes a thin shell over `lib::run(args)`. Keep `lib.rs` slim: module re-exports plus the top-level `run(args)` orchestrator (spec parsing, tracing init, cancellation-token plumbing, `JoinSet` wiring, top-level shutdown). Domain logic — framing, routing, transports — belongs in its module, not here.

## Testing strategy

**Every module ships with tests. Every phase is incomplete without them.** A phase that lands code with no tests is reopened, not merged.

Three test tiers, each with a clear home in the layout:

1. **Unit tests** — colocated in the source file via `#[cfg(test)] mod tests { ... }`. Test pure functions and small state machines: framer transitions, CRC computation, filter evaluation, msgid table merge, dedup hash window. Fast (< 10ms each), no I/O, no Tokio runtime required (or only `#[tokio::test(flavor = "current_thread")]` for the few async helpers). The framer in particular needs comprehensive unit coverage — it is the foundation of every other phase.

2. **Property tests** — `proptest` crate, also colocated. Required for the framer ("garbage in, no panic, no infinite loop, always resync within N bytes") and the filter evaluator ("blocklist always wins over allowlist on overlap"). Property tests catch the classes of bugs that example-based tests miss.

3. **Integration tests** — `tests/*.rs`. One file per feature area. Spin up real sockets on `127.0.0.1` using `tokio::net::TcpListener::bind("127.0.0.1:0")` (or the UDP equivalent) to get an OS-assigned port. Drive transports end-to-end. **No I/O mocks.** Mocking I/O hides exactly the reconnect, timeout, and partial-read bugs that integration tests exist to catch.

Specific test requirements per phase:

| Phase | Tests that must exist                                                                    |
|-------|------------------------------------------------------------------------------------------|
| 0     | CLI parser smoke (`cargo run -- --help`); EndpointSpec parser unit tests (all schemes).  |
| 1     | Framer unit + property tests; capture replay; `build.rs` XML parse + `crc_extra` computation tests; `<include>` resolution and cycle detection. Unit tests for the build-only modules under `build_support/` live in `tests/build_support.rs`, which `include!`s each file inside its own `mod` (one binary surfacing both sets). |
| 2     | UDP round-trip integration (two `udps:` instances exchange frames); peer idle-reap test. |
| 3     | TCP-client mid-stream disconnect → router survives; TCP-server accepts many; client kill → reconnect within N seconds; `tcpc:` reconnect drains and discards the pre-disconnect queue rather than replaying stale frames; `tcps:` bind to a pre-held port retries until the port frees. |
| 4     | Serial session loop over a `tokio_serial::SerialStream::pair()` PTY (Unix only): frame round-trip, slave-drop surfaces as `SessionOutcome::Disconnected`, cancellation during the reopen retry returns cleanly. |
| 5     | Routing: source-suppression, learn table populates, targeted vs broadcast decisions, In + Out filter end-to-end, sniffer sees everything, dedup suppresses redundant uplink, endpoint groups share the learn-set but **not** filters or stats (explicit asserts on both invariants), TX queue under sustained overflow increments `dropped_tx` exactly once per evicted frame, regardless of eviction source (router-side `force_push` overflow *and* writer-side `drain_and_discard` on reconnect both count, per the locked decisions); `tcpc:` reconnect drains and discards the pre-disconnect queue rather than replaying stale frames; `udps_peer_capacity` triggers LRU eviction when exceeded. |
| 6     | TOML config parses and round-trips; CLI+file merge precedence verified. |
| 7     | Priority TX queue under load; stats output schema; CI matrix green on linux-x86_64 + linux-aarch64 + windows-x86_64 + macos. |

Fixtures live in `fixtures/` and are version-controlled. Capture binaries should be small (< 1 MB each) and reproducible — document the source of each in a `fixtures/README.md`.

CI runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-features` on every push — all three are required to pass.

## Conventions for this repo

- Default to no comments. Names should carry the meaning; comments are for non-obvious WHY.
- **Every struct gets a brief `///` docstring** explaining its purpose — what it represents and why it exists, not how it's used. This is API documentation (visible in rustdoc), not a regular comment, and is exempt from the "no comments" rule above. One or two sentences. Keep the struct docstring about the **type as a whole** — do not list or describe individual fields in it. If a field needs explanation, attach the `///` to that field; the struct docstring is not the place for it. Self-explanatory fields stay undocumented.
- One `tracing::span` per endpoint; route decisions log at `trace!`.
- Errors are `thiserror` enums per module, joined at the binary boundary by a top-level `Error` enum with `From` impls. **No `anyhow`** anywhere in the tree, including `main`.
- No `unwrap()` outside of `main()` startup and tests.
- Bounded channels everywhere on the data path; dropped messages are counted, not buffered indefinitely.
- Integration tests live in `tests/`; they spin up real sockets on `127.0.0.1` with `tokio::net::TcpListener::bind("127.0.0.1:0")` to get an OS-assigned port.
- **Tests are not optional.** Every PR adds tests for the surface it touches. A bug fix without a regression test is incomplete.
- **`cargo fmt --check` and `cargo clippy -- -D warnings` are required to pass.** Style and lint regressions fail CI; fix them locally before pushing.

## Decisions (locked)

> **Reading note.** Locked decisions are forward design contracts. Some describe behaviour already implemented (Phases 0–3); others describe contracts the code will adopt as later phases land. The roadmap section is the source of truth for what's built today — phase tagging is implicit here. When a decision describes a struct or function that doesn't exist yet (e.g. `EndpointState`, `StatsEvent`, `IdentityFlags`), that means "Phase N will introduce it in this shape," not "this exists today and the code is wrong."

- **Project name:** **RMR** = **R**ust **M**AVLink **R**outer. Cargo crate stays lowercase `rmr` (Cargo convention); use RMR in prose, docs, logs.
- **License:** Apache-2.0. Matches mavlink-router; permissive with patent grant; lets us reuse anything Apache/MIT-compatible (notably rust-mavlink's CRC tables and the MAVLink XML files themselves).
- **Backpressure on slow endpoint:** drop-oldest with a per-endpoint dropped-frame counter exposed in stats. No disconnect on transient stalls — telemetry should stay fresh, and old frames are stale anyway.
- **MAVLink v2 signing:** pass-through only. RMR never verifies and never generates signatures. Signed frames are forwarded byte-for-byte. Non-negotiable — adding signing would turn RMR into a MAVLink node with a key, violating the "no identity" principle.
- **Serial hot-replug:** on read error / `ENODEV` / device-gone, the endpoint enters a reconnect state and polls the device path at ~1s intervals, reopening with the same baud and flow-control settings. Explicit improvement over mavlink-router.
- **DNS resolution:** re-resolve hostnames on every reconnect for `tcpc:` and `udpc:` endpoints. Survives DNS changes (k8s service migration, dynamic DNS, VPN routing changes) at the cost of one syscall per reconnect.
- **CRC validation:** validate CRC for known msgids at ingress; drop frames with bad CRC and count them per-endpoint. Unknown msgids cannot be validated (no `crc_extra` available) and are forwarded as-is. **Rationale:** drone links (SiK radios, LTE, mesh) are bandwidth-constrained; spending CPU on the high-bandwidth ingress side to avoid wasting bytes on the scarce outbound side is the right trade. Recipients validate end-to-end too, but by then the wasted radio time is already gone.
- **Dialect handling (2-layer model + fork path):**
  1. **Built-in** at compile time: `build.rs` consumes the `DIALECTS` list (defaults to `common.xml` + `ardupilotmega.xml`) and emits a `const` table of `(msgid → {name, crc_extra, min_payload_len, target_sys_offset?, target_comp_offset?})`. Covers the ~95% case with zero config.
  2. **Pass-through** for everything else: any msgid not in the table is forwarded as broadcast with CRC validation skipped. Never reject a frame for being unknown.

  Custom/proprietary dialects are handled by **forking**: drop the XML next to the vendored files, append the filename to `DIALECTS` in `build.rs`, rebuild. The existing `<include>` resolver, cycle detector, and `crc_extra` conflict checker apply to fork-added dialects automatically — a `crc_extra` mismatch with a built-in entry is a fatal build error, not a silent runtime override. There is intentionally no runtime `--dialect` flag: shipping an XML parser, include resolver, and conflict checker in the binary buys very little over pass-through, and the target audience (integrators building from source) loses nothing by rebuilding.
- **Vendored MAVLink XML provenance:** the XML files we depend on (`common.xml`, `ardupilotmega.xml`, and their transitive `<include>` dependencies) live **in-tree as regular committed files** under `vendor/mavlink/`. They are copied from `github.com/mavlink/mavlink` at a specific upstream release tag — the tag and copy date are recorded in `vendor/mavlink/UPSTREAM.md`. Updating is a deliberate, reviewable commit: clone upstream at the new tag, `cp` the relevant XML files into `vendor/mavlink/`, update `UPSTREAM.md`, run the full test suite. **No git submodule.** Rationale: `cargo publish`, `cargo install`, source tarballs, and fresh `git clone`s all work with zero submodule-init steps; the cost is a few KB of repo size and remembering to refresh files on upstream updates. `build.rs` reads `common.xml` + `ardupilotmega.xml` (plus anything in the `DIALECTS` list) from `vendor/mavlink/` and panics with an actionable message naming the missing file if anything referenced (top-level or `<include>`) is absent.
- **IPv4 first, IPv6 supported but not a focus.** Endpoint scheme parsers accept IPv4 dotted-quads, hostnames, and bracketed IPv6 literals (`udps:[::]:14550`, `tcpc:[2001:db8::1]:5760`). All TCP and UDP unicast endpoints work over either family; address-family is inferred from the parsed address (or from DNS resolution result for hostnames — prefer v4 on tie, since most MAVLink ecosystems are v4-only). When bound to `[::]`, `udps:` and `tcps:` are made dual-stack on both Windows and Linux by explicitly setting `IPV6_V6ONLY=0` via `socket2` — without this, Windows defaults to v6-only and Linux defaults to v4+v6, which is a portability footgun. IPv6 paths must not gate v1; if an IPv6-specific bug blocks a release, ship v4-only and fix in a follow-up.
- **No UDP broadcast.** `udpb:` is not in scope for v1. The supported UDP schemes are `udps:` (server with multi-peer learning) and `udpc:` (client with reply-source latching). MAVLink-over-broadcast is rare on real fleets (broadcast doesn't traverse routers, GCSs and SBCs usually configure explicit unicast peers); the multicast analogue is also out of scope.
- **`udpc:` reply-source latching (IP-bound, with idle revert).** A `udpc:` endpoint begins by sending to its configured `host:port`. Latching is **IP-scoped**: only an inbound packet whose source IP matches the configured (DNS-resolved) host's IP causes the destination to update to that packet's `(ip, port)`. Port may differ — that's the GCS-on-ephemeral-port case we must support — but packets from any other IP are dropped at ingress (counted in `in_filter_drops` per the union-counter rule in the Phase 5 In-filter bullet, logged at DEBUG). Diverges from `mavlink-router`'s permissive "first speaker wins" latch on purpose: prevents accidental cross-talk and casual hijack on shared networks. After latching, if no inbound arrives from the latched peer for `?latch_idle_secs=N` (default 30s), the router treats the latch as stale, re-resolves DNS, and reverts the destination to the configured `host:port`. Handles GCS restarts on a fresh ephemeral port. Host with no resolvable A/AAAA record on revert: keep the previous resolved IP and log at WARN; do not block forever. **Revert is not a transport-down event** — the socket remains bound, only the send destination changes — so the `udpc:` endpoint task does not write `Reconnecting` on revert; state stays `Connected`. The task writes `Reconnecting` only on `send_to` errors and DNS-resolve failures that prevent any send at all.
- **TCP keepalive:** `SO_KEEPALIVE` is enabled on every `tcps:` accepted client and every `tcpc:` socket, with **OS default** idle/interval/probes. Linux defaults are ~2 hours of idle before the first probe — useful as an absolute backstop, *not* useful for fast dead-peer detection. Operators who need quick detection should tune the kernel-wide `tcp_keepalive_*` sysctls; if we later need per-endpoint params we will wire `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT` through `socket2`.
- **Bind/open failure at startup is not fatal.** Configuration errors (unknown query keys, malformed addresses, duplicate `#name`) abort at parse time. Runtime resources are treated as may-be-unavailable: a serial device that isn't plugged in, a TCP listen port that's still bound by the previous process, a UDP bind that conflicts — each transitions the affected endpoint into its normal reconnect/retry loop and the router proceeds. Each failed attempt logs at WARN. Rationale: a router that crashes on startup because one of N endpoints is briefly unavailable is operationally worse than one that comes up partially and recovers.
- **MSRV `1.85`** declared via `rust-version` in `Cargo.toml` (Edition 2024's minimum). Bump deliberately and only when a feature being adopted requires it.
- **`#name` validation:** explicit `#name` must match `[A-Za-z0-9_-]{1,64}`. Anything else is a parse-time error. Keeps names safe in logs, JSON stats keys, file paths, and `tracing` span fields.
- **Drop-oldest TX queue implementation:** `Arc<crossbeam_queue::ArrayQueue<Bytes>>` (bounded, default cap 256) paired with `Arc<tokio::sync::Notify>` for wakeups. The router enqueues with `force_push(b)` which atomically inserts and returns the displaced element on overflow; that return value increments `dropped_tx` and is dropped. After each push the router calls `notify.notify_one()`. Writer task pops in a loop and `await`s `Notify` when empty. Picked over `tokio::sync::mpsc` (the obvious first instinct — but its `Receiver` is single-consumer and owned by the writer task, so the sender side has no API to evict the head on `Full`; the "thin wrapper that calls `recv()`" pattern doesn't actually express in Rust), over `Mutex<VecDeque>` + `Notify` (more code, single-mutex contention), and over `flume` (extra dep overlapping `crossbeam-queue` in capability).
- **Reader → router channel: bounded + backpressure (not drop-oldest).** Single shared `tokio::sync::mpsc<(EndpointId, Bytes, ParsedHeader)>` with `ingress_queue_frames` capacity (default 1024). Senders **await on full**. Rationale: the router task does a few dozen ns of work per frame; if it ever falls behind enough to fill 1024 entries, that is a real problem (CPU starvation, lock contention upstream, GC pause in a debugger, etc.) and we want it surfaced as visibly stalled readers rather than silent ingress drops with no obvious source-of-truth counter. Per-endpoint writer queues use drop-oldest because a slow consumer is an expected steady-state condition; a slow router is not.
- **TX queue on disconnect: drain and discard, never replay.** When a writer task detects a disconnect (write error on `tcpc:`, `serial:` device gone, `tcps:` child socket closed), it stops popping from its queue and enters its reconnect / cleanup path. The router keeps pushing fresh frames; `force_push` keeps evicting the oldest; `dropped_tx` keeps incrementing as overflow continues. On reconnect — before resuming normal pop — the writer drains the queue completely (`while q.pop().is_some()`), incrementing `dropped_tx` by the drained count, and only then begins consuming new frames. Rationale: telemetry buffered during a multi-second outage is stale by the time the link is back; replaying it ahead of fresh frames actively misleads the receiver (e.g. a GCS sees old positions before current ones). Aligns with the "old frames are stale anyway" rationale already documented for backpressure.
- **`udps:` peer admission cap:** per-listener `udps_peer_capacity` (default 256). When a packet arrives from a never-seen source address and the peer set is full, the peer with the oldest last-seen is evicted — its routing endpoint, learn-set, and stats are dropped — and the new peer is admitted. **Eviction handling of in-flight TX frames:** before the parent listener drops the evicted peer's handles, it calls `tx_queue.drain_and_discard()` on the peer's queue; any pending frames count into `dropped_tx` per the standard rule, and the final stats line emitted by the stats task (on `StatsEvent::Finalize`) reflects the incremented counter. Bounded memory and bounded stats-output volume under misbehaving or spoofed-source-address traffic; the `idle_secs` reaper still handles the steady-state-tidy case. Without this cap, a flood from random ephemeral ports would grow the peer table (and the stats-line count) unboundedly until the OS killed the process.
- **MAVLink header parsed in the reader task.** The reader emits `(EndpointId, Bytes, ParsedHeader)` on the router channel. The router consumes the parsed header directly and never re-parses. The bytes the header was parsed from are still cache-hot in the reader; parsing twice would cost ~50ns/frame for nothing.
- **`learn_capacity` / `seq_tracker_capacity` eviction:** LRU by last-seen. When the per-endpoint flat structure is full, the entry with the oldest last-seen timestamp is evicted on insert. Survives chatty-then-quiet sources gracefully.
- **Stats counter units:** both frames and bytes are emitted: `rx_frames`/`tx_frames` for routing health, `rx_bytes`/`tx_bytes` for link-budget visibility. Maintaining both is cheap (one extra u64 add per frame) and either alone is insufficient for the typical operator question.
- **Stats `state` field:** each per-endpoint stats object carries `state` ∈ `{connected, reconnecting, idle, down}`. `idle` is emitted once when a learned UDP peer ages past `idle_secs`; `down` is emitted once before a `tcps:` child is removed. Dashboards/alerts can act on link health without parsing logs.
- **CI lint discipline:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-features` all required to pass in CI from Phase 0. Style and lint regressions fail builds; fix locally before pushing.
- **Error handling:** `thiserror` per module, joined at the binary boundary by a top-level `Error` enum with `From` impls. No `anyhow` anywhere — including `main`. Module errors are first-class types throughout.
- **`serde_json` for stats output only.** Not used for any internal wire format or config schema (TOML covers config). One emit per stats period per endpoint; negligible cost.
- **Sequence-loss estimation:** the router tracks, per source endpoint, a small `(srcsys, srccomp) → last_seq` map. On each valid inbound frame, gap = `(seq - last_seq - 1) mod 256`. Gaps below a sanity threshold (default 64) are added to a per-endpoint `rx_lost_est` counter; gaps at or above the threshold are treated as a source restart / long silence and only reset the tracker without inflating the counter. Best-effort only — seq wraps and reordering can mis-attribute a few frames — but it is the standard MAVLink link-quality signal and worth surfacing. Entry lives in a fixed-capacity flat structure on the source endpoint (default `seq_tracker_capacity = 32`) with **LRU eviction by last-seen** when full — independent of the learn table's eviction (capacities are independent so a chatty source doesn't lose its seq history just because it dropped off the learn LRU).
- **Priority TX queue:** stretch (Phase 7). Two-class per-endpoint queue (HIGH / LOW) with a `low_priority_msgids` config list. High drains first with a weighted ratio (e.g. 4:1) to avoid absolute starvation. Doesn't reorder within a class. Default empty list = current single-queue behaviour. Phase 5 builds the single-queue TX so the retrofit is straightforward.
- **`udps:` peer expiry:** peers expire on inactivity, default 60s, configurable per endpoint via `?idle_secs=N`. Matches mavp2p behaviour. Without expiry, a long-lived `udps:` accumulates dead peers and burns memory + bandwidth sending to nowhere.
- **TCP-server client cap:** none. RMR accepts connections until the OS refuses (open-fd limit hit). Rationale: a router operator who needs limits should set them at the OS level (`ulimit`) or via a reverse proxy; baking a soft cap into RMR adds config surface for a knob almost nobody will tune.
- **Frame ownership across tasks:** `bytes::Bytes`. The framer reads into a per-endpoint `BytesMut` using the standard `tokio_util` codec pattern — `split_to(frame_len).freeze()` on each complete frame, `reserve(n)` to keep headroom for the next read; the allocation is reclaimed when all writers have dropped their `Bytes` clone. From that point every reader→router mpsc send and every router→writer `ArrayQueue` push is an Arc bump — zero copy. Picked over plain `Vec<u8>` (would copy N times on fan-out), `Arc<[u8]>` (no slicing ergonomics), and pooled slabs (over-engineering for v1).
- **Routing granularity for sub-connections:** TCP `tcps:` children and UDP `udps:` peers are each their own *routing endpoint* with independent learn-set, loop-prevention scope, and stats. The parent listener exists only as a configuration / supervision unit; filters declared on it apply to every child by reference. This is the mavlink-router model and is necessary for correct loop prevention with multiple clients on one listener.
- **Endpoint groups:** declared implicitly by `?group=name` (CLI) or `group = "name"` (TOML). Members share *only* the learn-set; filters and stats remain per-endpoint. There are no `[groups.X]` TOML blocks in v1.
- **Dialect XML includes:** `build.rs` resolves `<include>...</include>` recursively, search path = the including file's directory, with cycle detection (fatal if detected). The vendored `common.xml` + `ardupilotmega.xml` already follow this convention; forks adding their own XML inherit the same resolver.
- **`crc_extra` source:** computed locally from XML field types/names in size-sorted order (extension fields skipped) at build time by `build.rs`. Avoids depending on `rust-mavlink`.
- **msgid table representation:** `const SORTED: &[(u32, MsgEntry)]` emitted by `build.rs`, sorted by msgid; runtime lookup is binary search. Single tier — no parallel runtime Vec, no two-tier lookup. Avoids `phf` build-dep.
- **Dedup hash + storage:** xxh3-64 (`twox-hash`) over the framed bytes. **The window is a single global structure owned by the router task** — not per-source-endpoint. Rationale: the motivating use case is redundant uplinks (LTE + RFD900, primary + standby) delivering the same vehicle frame on two different ingress endpoints; only a global window catches that cross-endpoint duplicate. *Note on overlap with loop-prevention:* loop-prevention at the per-destination step already drops a frame at every destination whose learn-set contains the source `(sysid, compid)`, which suppresses *some* redundant-uplink duplicates after both uplinks have learned the source. Dedup is not redundant: it drops the duplicate at the router *before* learn-set updates and per-destination evaluation, saving the N-1 per-destination filter/target-match passes the second copy would otherwise trigger, and preventing learn-set thrash when the two uplinks alternate which delivers first (without dedup, every alternation re-touches `last_seen` on the shared learn entry). Membership is indexed in a `HashSet<u64>` for O(1) lookup; eviction order is driven by a parallel fixed-capacity FIFO ring of `(u64, Instant)` (`dedup_window_capacity`, default 4096 entries total). On insert: expire TTL-stale entries from the ring head (also removing them from the set), then if the ring is at capacity evict the oldest, then push the new entry into both. Bounded memory regardless of frame rate; lookup cost stays constant regardless of window size — a plain linear scan of 4096 entries would be ~32 KB scanned per frame, which is L1-resident but real work the router doesn't need to do. Rejected: per-source-endpoint windows — would silently break the redundant-uplink case. Rejected: per-group windows (with a fallback to per-endpoint) — adds lifecycle complexity for marginal flexibility; if a deployment wants scoped dedup, it can omit `DeDuplicationPeriod` on endpoints that shouldn't share.
- **Build-time XML parser:** `quick-xml` declared in `[build-dependencies]`. Build-time only — not a runtime dependency.
- **Stats output:** JSON-Lines on **stdout**, one object per `--stats-interval` (default 5s), enabled by `--stats`. Logs (`tracing`) go to **stderr** — human-readable by default, JSON via `--log-format=json`. Stats and logs never interleave on the same stream.
- **Stats sink architecture:** stats are emitted from a **dedicated stats task**, fully factored off the router. The stats task owns three things: (a) a registry mirror — `HashMap<EndpointId, (name, Arc<EndpointStats>)>` — fed by the router via `stats_event_tx`, (b) the interval timer (`tokio::time::interval(stats_interval)`), and (c) the stdout writer. **Channel sizing:** `stats_event_tx` is a `mpsc<StatsEvent>` sized at spawner-construction time as `max(64, 2 × N)` where `N = (number of top-level endpoints declared on CLI/TOML) + sum(udps_peer_capacity for each udps: listener) + sum(tcps_peer_budget for each tcps: listener)`, with `tcps_peer_budget` defaulting to 64 per listener (tcps has no hard cap; this is a sizing hint, not an enforced limit — the router blocks briefly on `stats_event_tx.send().await` if the burst exceeds the hint, which is acceptable on lifecycle events). Lifecycle events are rare relative to frame traffic; the router `send`s on it but does **not** wait for any acknowledgement. `stats_line_tx` is `mpsc<StatsLine>` with capacity `stats_queue_lines` (default 256). **Router responsibility:** on every `EndpointAdded`/`PeerAdded` the router forwards `StatsEvent::Register { id, name, stats: Arc<EndpointStats> }` (fire-and-forget — `await` only completes when the mpsc has capacity, which is the normal case); on every `PeerRemoved` (and for top-level endpoints, on the cancel-token-triggered shutdown sweep) the router writes `state = Idle | Down` on the shared Arc *first*, then forwards `StatsEvent::Finalize { id }` and drops its own registry handle. The router never walks per-endpoint stats, never collects snapshots, never touches stdout, and never awaits a stats-task ack. **Stats task interval tick:** walks its registry, builds one `StatsLine` per registered endpoint, sends each to `stats_line_tx` (drop-oldest on overflow; increment `stats_dropped` and emit at most one WARN per interval). **Stats task on `StatsEvent::Finalize { id }`:** emits one final synthetic `StatsLine` via a **separate path that bypasses drop-oldest** — `try_send`, then if full `await` the send with a per-line 100ms timeout (genuinely dropped final lines log at WARN, no silent loss of the authoritative end-state). Then removes the entry from the registry mirror. **On shutdown:** the stats task is a top-level `JoinSet` entry and gets its own private 2s drain budget (see per-task drain bullet) — it processes any pending Finalizes within that window, emitting final lines via the bypass path on a best-effort basis. **On `BrokenPipe` writing stdout:** the stats task logs once at WARN and continues running (still drains the channels, drops new lines). Rationale: factoring the timer + snapshot loop off the router preserves the "few-dozen-ns per frame" justification for bounded reader→router backpressure (see decision below); fire-and-forget on `stats_event_tx` keeps the router off the synchronous-handoff path so the 5s drain budget is never spent serial-awaiting per-endpoint Finalize acks.
- **CLI + TOML merge:** TOML endpoints come first, CLI endpoints append in argv order. Endpoint `#name` is unique across the combined set — collision is a fatal error, not a merge. CLI globals override TOML globals per-key. Endpoint names auto-generated from `scheme-addr-port` when `#name` is omitted.
- **Strict validation at startup:** unknown query-string keys are fatal with a "did you mean" suggestion. Duplicate endpoint addresses log at WARN.
- **Reconnect backoff:** `tcpc:` uses capped-exponential (250 ms → 30 s) with ±20% jitter, per-endpoint overrides via `?reconnect_initial_ms=`/`?reconnect_max_ms=`. Serial hot-replug stays at a fixed 1s poll (devices appear or they don't — backoff doesn't help).
- **Shutdown timing:** on cancellation each task gets up to 2s to drain (final TX flush, last frame write); after a 5s overall wall-clock budget any remaining tasks are `abort()`ed via `JoinSet` and any missed deadline is logged at WARN. Bounded shutdown, recoverable from stuck syscalls.
- **v2 zero-trim + target offsets:** if a known msgid's `target_system`/`target_component` offset lies beyond the (possibly zero-trimmed) `payload_len`, the target byte is implicitly 0 (broadcast). The router must check the offset against `payload_len` before reading.
- **`serial:` grammar — rsplit on last `:`, validate baud is numeric, `,` is an accepted alias.** A `serial:` spec body is parsed by `rsplit_once` on the last `:` *or* `,` (whichever appears last). The right side must `parse::<u32>()` as the baud; the left side is the device path verbatim. So `serial:/dev/ttyUSB0:921600`, `serial:/dev/ttyUSB0,921600`, `serial:COM3:115200`, and `serial:COM3,115200` all work. A spec with no separator (`serial:COM3`) or a non-numeric right side (`serial:/dev/foo:abc`) is a parse-time error with a clear message. Rationale: real-world serial paths never contain `:` or `,` in practice (Linux `/dev/*` paths, Windows `COM*` / `\\.\COM*` names, by-id symlinks); a numeric-baud check eliminates the residual ambiguity. Preserves both forms the doc has always advertised and matches mavp2p's convention without diverging.
- **`crc_extra_for_message` is single-source-of-truth.** The algorithm lives in `src/mavlink/crc_extra.rs`. `build.rs` consumes it via `include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mavlink/crc_extra.rs"))` — Cargo forbids `build.rs` from `use`ing items in the crate it is building, so include-from-source is the standard pattern. The runtime crate also exports the same function (`pub(crate) fn crc_extra_for_message(...)`) so unit tests can assert known `(msg_name → crc_extra)` pairs match published MAVLink reference values (e.g. `HEARTBEAT` = 50, `SYS_STATUS` = 124). No duplication, no sibling crate.
- **Build-only modules live under `build_support/`, not under `src/`.** `crc_extra.rs` is the *only* `include!`-from-source file that stays in `src/mavlink/` — because its algorithm is also called at runtime by tests. Everything else that exists solely to drive `build.rs` — currently `dialect_parse.rs` (pure transforms over parse results) and `xml_loader.rs` (generic include-graph walker with cycle detection) — lives under `build_support/` at the repo root. `build.rs` `include!`s them; their unit tests run via `tests/build_support.rs`, an integration test binary that `include!`s each file inside its own `mod` so the two `#[cfg(test)] mod tests { ... }` blocks don't collide. Rationale: the runtime crate's module tree should reflect what the binary actually does. A `pub(crate) mod xml_loader;` in `src/mavlink/mod.rs` that exists purely so `cargo test` finds the file's unit tests is documentation debt — a future reader has to dig through git history to learn the module is dead at runtime. Files in `build_support/` must remain self-contained (no `use crate::*`, no sibling `mod`) because they're `include!`d into two compilation contexts (build.rs and the integration test binary). Rejected alternatives: a `build-support` workspace member crate (doubles compile units and complicates `cargo publish` for two files of pure functions); a `xtask` (overkill — these aren't build orchestration, they're pure transforms).
- **Endpoint-spec fragment-before-query is intentional.** Spec grammar is `SCHEME:SPEC[#name][?key=val&...]` — `#name` precedes `?`, inverting URL fragment-after-query convention. Locked because every example in this doc relies on it and the parser is simpler when `#` is searched before `?` is parsed.
- **Sub-endpoint naming convention:** `tcps:` children and `udps:` peers inherit the parent's `#name` (explicit or auto) and append `/ip-port` from the peer's source address — e.g. `gcs/127.0.0.1-54321`, `bus/192.168.1.10-14550`, `tap/[2001:db8::1]-14550` for IPv6. The composite name is what appears in stats `endpoint` and in `tracing` span fields. Names are deliberately *not* stable across reconnects: a re-accepting client gets a fresh name reflecting its new ephemeral port, which keeps the loop-prevention and learn-set state per-connection rather than per-peer (the safer default for redundancy and NAT scenarios). Operators who want per-peer aggregation should compute it from the parent prefix on the consuming side.
- **Filter list grammar:** comma-separated decimal integers and inclusive decimal ranges (`33,100-150,32`). Hex literals, symbolic msgid names, and wildcards are intentionally not accepted — keeps the parser tiny, decouples CLI/TOML parsing from the const msgid table, and avoids the case where a fork rebuilds with a different dialect and silently changes what `HEARTBEAT` means. Operators who want hex can convert; integrators rebuilding with custom dialects are unaffected.
- **Port reuse on `tcps:` and `udps:` binds:** `SO_REUSEADDR = on`, `SO_REUSEPORT = off` (where the OS exposes them separately). Cross-platform: same behaviour on Linux, Windows, and macOS. REUSEADDR lets a fresh RMR rebind on the same port after a crash without waiting out TIME_WAIT — the operationally useful case. REUSEPORT (Linux/BSD only) would allow two RMR processes to bind the same port and split incoming frames between them, which is a worse failure mode than "second instance refuses to start" for a router. Set via `socket2` before listen/bind.
- **Tokio runtime: multi-threaded scheduler with default worker count.** `#[tokio::main]` with no `flavor` override; the default multi-thread runtime uses `num_cpus` workers. Rationale: reader tasks can be blocked inside transport syscalls (especially `serial:` reads on Linux's blocking VFS path) without starving the router task; the router task remains single-task and is therefore still the sole owner of routing state, but it can run on a different OS thread from the reader that's about to feed it. The doc-level "single-threaded router" wording always meant "one task processing serially," not "pinned OS thread."
- **`udpc:` multi-A-record DNS:** when the configured host resolves to multiple IPs (DNS round-robin, dual-stack), latching accepts inbound packets from **any** currently-resolved IP for that host. Re-resolution happens on send-error and on `latch_idle_secs` revert; the resolved IP set is refreshed at the same time. Single-A-record hosts (the common case) collapse to the existing behaviour.
- **Sniffer + dedup ordering:** ingress dedup runs *before* the per-destination decision, which is where the sniffer override lives. Sniffers therefore see post-dedup traffic — the same set of frames the router accepted for routing. This is intentional: a sniffer is a diagnostic tap for *what the router did*, not a wire-tap. Operators who need raw pre-dedup traffic should disable `dedup_ms` on that ingress or use a passive packet capture outside RMR.
- **`twox-hash` feature pin:** dependency declared as `twox-hash = { version = "2", default-features = false, features = ["xxhash3_64"] }`. xxh3-64 is behind the `xxhash3_64` feature in twox-hash 2.x; the default features pull in legacy variants (`xxhash32`, `xxhash64`, `xxhash3_128`) plus a `rand` dependency we don't need.
- **Session loop is one task per connection, not two; shared across transports.** `serial:`, `tcpc:`, and `tcps:` accepted children all run their read and write halves inside a single `tokio::select!` loop in `src/endpoint/session.rs::run_session<S: AsyncRead + AsyncWrite>`, multiplexing `tokio::io::split(stream)` halves under one task. The Architecture diagram's "one task per endpoint (read), one task per endpoint (write)" wording reflects the UDP server's per-peer writer task pattern; TCP and serial collapse the two because the kernel buffer (zero-window for TCP, tty/usbserial FIFO for serial) flows backpressure to the peer regardless of where the writer lives, and a combined select keeps the per-connection lifecycle (disconnect, cancellation, queue-drain) localised. The UDP-server peer-writer-task split remains because UDP has no per-peer kernel socket to backpressure through.
- **`Spec` + `Wiring` spawner contract.** Every endpoint `run()` takes two arguments: a `Spec` struct carrying the per-endpoint identity (addresses, EndpointId, name, parsed config) and a `Wiring` struct carrying the shared plumbing (router channels, cancellation token, and — for parent listeners — the `EndpointIdAllocator` and the `EndpointEvent` channel). For top-level endpoints the spawner constructs the `TxQueue` and `EndpointStats` in advance and hands clones to the router via the wiring; for sub-endpoints (`tcps:` children, `udps:` peers) the spawning parent constructs the `TxQueue` and `Arc<EndpointStats>` on accept/admission and hands them to the router via `EndpointEvent::PeerAdded`. The router is "sole owner of mutable routing state" but does **not** allocate per-child handles — the parent owns the child's lifecycle from creation to `PeerRemoved`.
- **`PeerRemovalReason` taxonomy vs stats `state` field.** `PeerRemovalReason::{Idle, LruEvicted, Disconnected, ListenerShutdown}` is the internal cause-of-removal signal the router consumes on `PeerRemoved`. It maps to the user-visible stats `state` field as follows: `Idle` → final `idle` line then removal; `LruEvicted`, `Disconnected`, and `ListenerShutdown` → final `down` line then removal. The taxonomy is deliberately finer than `state` so logs and tracing fields can distinguish "the peer aged out" from "the listener is shutting down" without parsing free-text.
- **`tcps:` bind-retry shares the `tcpc:` curve, no per-listener override.** A `tcps:` listener whose initial bind fails (port held by a previous process, transient OS refusal) enters the same capped-exponential backoff (`reconnect_initial_ms = 250` → `reconnect_max_ms = 30_000`, ±20% jitter) as `tcpc:`. Unlike `tcpc:`, **`tcps:` does not expose `?reconnect_initial_ms=` or `?reconnect_max_ms=` query overrides** — once bound the listener stays up for the life of the router, so the curve only matters during the (rare) startup race and the defaults are operationally sufficient. Same reasoning applies to `udps:`.
- **`tcpc:` connect timeout is hard-coded at 10s.** Each `TcpStream::connect` attempt inside `dial_with_dns` is wrapped in a 10-second `tokio::time::timeout`. Not exposed as a per-endpoint knob in v1 — networks that need longer almost certainly need the reconnect loop's backoff to kick in instead. Wired through to the cancellation token so shutdown during a hanging connect is bounded by the drain budget, not the connect timeout.
- **Endpoint registration is symmetric, and ordering is enforced by biased select.** `EndpointEvent::EndpointAdded` is the top-level analogue of `PeerAdded` — same `event_tx` channel, same payload shape. The spawner constructs `Arc<EndpointStats>`, `TxQueue`, and `IdentityFlags` for top-level endpoints; the parent listener does the same for sub-endpoints. The router never allocates a routing-endpoint handle; it only reacts to events. **Registration-before-frame is enforced by two rules together:** (a) the producer `await`s `event_tx.send(...)` to completion before any task holding the matching `EndpointId` can call `frame_tx.send(...)`; (b) the router's main loop is `tokio::select! { biased; ev = event_rx.recv() => …, fr = frame_rx.recv() => … }`, so every available event drains before the next frame is taken. Rejected: collapsing both into a single `mpsc<enum RouterMsg { Frame, Event }>` — would force every per-frame send to construct an enum tag and would entangle frame-rate backpressure with event delivery; `event_tx` must never be saturated by frame traffic. **Stats-task registry mirror:** on receiving `EndpointAdded`/`PeerAdded`, the router forwards `StatsEvent::Register { id, name, stats: Arc<EndpointStats> }` to the stats task's `stats_event_tx` mpsc; on `PeerRemoved` (or top-level shutdown sweep) the router writes the final `state` then forwards `StatsEvent::Finalize { id }`. Forwarding is fire-and-forget — the router does not await a stats-task acknowledgement. The stats task maintains its own registry mirror from these messages and uses its own 2s drain budget to emit any pending final lines on shutdown — see the Stats sink architecture decision. On shutdown the router walks its top-level registry, writes `state = Down`, and forwards `Finalize` for each before dropping its handles; the stats task is responsible for getting the final lines out within its own drain window. Only sub-endpoints emit `PeerRemoved`. No `EndpointRemoved` variant in v1.
- **Filters, group, sniffer, and learn/seq capacities travel with the `*Spec`, not the `*Wiring`.** They're per-endpoint identity, not per-plumbing — so each `*Spec` carries them in a typed `IdentityFlags` substruct: filters stored as the spec parser already produces them (`Vec<MsgIdRange>` for msgid axes, `Vec<U8Range>` for src_sys / src_comp axes — ranges scanned linearly per the Filter cost paragraph), plus `sniffer: bool`, `group: Option<Arc<str>>`, `learn_capacity: usize`, `seq_tracker_capacity: usize`. The spawner builds `IdentityFlags` once from the parsed `CommonQuery`, hands a clone to the reader (for In-filter evaluation) and another clone via `EndpointAdded`/`PeerAdded` (for the router's Out-filter, sniffer, target-match, and learn/seq state). Sub-endpoint inheritance is by clone from the parent's flags at spawn time. Rejected: sharing via `Arc<IdentityFlags>` — Phase 5 has no mutator, and a shared `Arc` invites future code to assume cross-task mutation that we don't want. Phase 6's SIGHUP-reload stretch goal, if pursued, replaces the by-value clone with `ArcSwap<IdentityFlags>` at every holder so live endpoints can adopt new filters without teardown; that change is local and does not invalidate Phase 5 call sites. `IdentityFlags` lives in `src/endpoint/identity_flags.rs`; the `Filters` data half lives next to it in `src/endpoint/filters.rs`.
- **`EndpointStats.state` is an `AtomicU8` with split-authority writes.** The enum is `EndpointState { Connected = 0, Reconnecting = 1, Idle = 2, Down = 3 }`. **The `Connected = 0` discriminant is load-bearing:** `EndpointStats::default()` therefore lands in state `Connected` with no explicit store, which is the right initial value for sub-endpoints whose admission *is* the transport-up event (UDP peers, TCP accepted clients on a healthy socket). **Top-level endpoints construct via `EndpointStats::new(EndpointState::Reconnecting)`**, a tiny helper that sets the slot before the `Arc` is published anywhere — closing the brief default-then-store window where a concurrent stats snapshot could observe `Connected` for an endpoint that hasn't bound yet. **"Endpoint task" — definition.** For `tcpc:`, `udpc:`, and `serial:` the endpoint task is the single per-endpoint task that runs the reader/writer select loop (TCP collapses reader+writer into one task per the locked TCP-session decision; UDP-client and serial follow the same shape). These are the endpoint types **that have Connected↔Reconnecting transitions**: they keep their `EndpointId` across reconnects, so flipping state back to `Reconnecting` on disconnect-with-retry is the natural model. For `tcps:` accepted children, the endpoint task is the per-child session task — but `tcps:` children **do not reconnect**: disconnection tears down the child (router writes `Down`, parent drops the handle), and a fresh accept produces a brand-new child with a brand-new `EndpointId`. So a `tcps:` session task only ever observes `Connected → Down` and never writes `Reconnecting`. For `udps:` peers there is **no per-peer reader task** (the listener owns `recv_from`); the per-peer writer task is the *only* per-peer task, and it carries the peer's identity but does not own state-write authority — UDP peer state is initialised via `default()` at admission and never transitioned by the writer, so the rule "endpoint task writes Connected/Reconnecting" is vacuous for UDP peers (they sit at `Connected` from admission until the router writes `Idle`/`Down`). **Write authority is strictly split, bidirectional:** for every endpoint *type that has Connected/Reconnecting transitions*, the endpoint task writes `Connected`/`Reconnecting` and *must not* write `Idle` or `Down`; the router task writes `Idle`/`Down` (in the same step as it processes `PeerRemoved { Idle }` → `Idle`; `PeerRemoved { Disconnected | LruEvicted | ListenerShutdown }` or top-level shutdown → `Down`) and *must not* write `Connected` or `Reconnecting`. The router forwards `StatsEvent::Finalize` to the stats task after its state write; the stats task emits one final synthetic line before the router drops the `Arc<EndpointStats>` from its registry. **Happens-before on shutdown:** once an endpoint task observes the cancellation token, it must not write `state` again; this guarantees the router's `Down` write is the last write to the slot even though both tasks share it. *Consequence:* interval-driven stats snapshots that land inside the drain window may show the pre-cancel state (e.g. `Connected` for a socket that's already half-torn-down) until the router's final synthetic snapshot fires — operators read the final synthetic line as the authoritative end-state, not the trailing interval snapshot. **UDP peer admission specifically:** since UDP has no transport-up event, the parent `udps:` listener relies on the `default()` constructor putting state at `Connected`, and the peer task never enters `Reconnecting` (admission → `Connected`, reap → `Idle`, listener-shutdown / LRU → `Down`). `Idle`/`Down` are never observed on a regular interval-driven snapshot — only on lifecycle-final synthetic snapshots — which is what makes the existing `PeerRemovalReason` → stats `state` mapping (`Idle → final idle line then removal`) operationalisable. Rejected: `ArcSwap<EndpointState>` — overkill for a 4-variant enum that's `Relaxed`-loaded; a 1-byte atomic is the cheapest signal that compiles. Rejected: a `dyn StateSink` callback shim — adds indirection on a hot-path-adjacent struct for no win. Rejected: making the router `await` each endpoint's `JoinHandle` before its `Down` write — adds shutdown-time coordination, risks the 5s drain budget if a task hangs, and is unnecessary once the cancel-then-no-write rule is in place.
