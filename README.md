# RMR — Rust MAVLink Router

A minimal, high-throughput MAVLink router. Forwards MAVLink frames between
serial, UDP, and TCP endpoints with a learned routing table that improves
targeted delivery over pure broadcast. Bytes in, bytes out — RMR owns no
MAVLink identity, never emits a `HEARTBEAT`, and parses no more of a frame
than it needs to in order to route it.

RMR is **not** a GCS. It never emits `HEARTBEAT`, never requests data
streams, never rewrites sysid / compid / seq, never validates or
generates v2 signatures (signed frames pass through opaquely), and has
no plugins, REPL, or admin API. If your use case needs any of those,
it belongs in a separate tool, not in the path of every frame.

Design rationale and the full feature surface live in
[CLAUDE.md](CLAUDE.md).

## Install

### Prebuilt binaries

Each release attaches binaries for Linux (`x86_64` and `aarch64`, both
`musl`-static and dynamically-linked `gnu`), macOS (`x86_64` and `aarch64`),
and Windows (`x86_64`) to the GitHub Release. A `SHA256SUMS` file is
attached next to the binaries; verify with:

```sh
sha256sum -c SHA256SUMS
```

### Docker

Multi-arch images (`linux/amd64`, `linux/arm64`) are published per
release to `ghcr.io/sheijningen/rmr` with tags `:<X.Y.Z>`, `:<X.Y>`, and
`:latest`. Pin to `:<X.Y.Z>` in production.

```sh
docker run --rm -i --network host \
  ghcr.io/sheijningen/rmr:0.1.0 \
  udps:0.0.0.0:14550#bus tcpc:192.168.144.15:5760#vehicle
```

`--network host` is required for `udps:` listeners learning peers from
arbitrary source addresses; bridge networking with `-p` mappings works
for unicast-only setups. Pass serial devices through with
`--device=/dev/ttyUSB0:/dev/ttyUSB0`.

## Quick start

Route between a flight-controller serial link and a GCS over TCP:

```sh
rmr serial:/dev/ttyUSB0:115200#fc tcps:0.0.0.0:5760#gcs
```

Same setup driven from a TOML file:

```toml
[[endpoints]]
type = "serial"
name = "fc"
path = "/dev/ttyUSB0"
baud = 115200

[[endpoints]]
type = "tcps"
name = "gcs"
bind = "0.0.0.0:5760"
```

```sh
rmr -c rmr.toml
```

More examples and use cases under [`examples/`](examples/).

## How RMR routes (the 30-second mental model)

Every endpoint declared to RMR — a serial port, a UDP/TCP listener, a UDP/TCP
client — is a peer that **sends frames in and receives frames out**. RMR keeps
no MAVLink identity of its own; it watches what flows past and learns where
each `(sysid, compid)` pair lives by remembering the endpoint a frame came in
on.

When a new frame arrives, RMR:

1. **Learns** the source `(sysid, compid)` against the ingress endpoint
   (other endpoints in the same `?group=` share this learn-set).
2. **Forwards to every other endpoint**, with two short-circuits per
   destination:
   - **Loop-prevention** — if the destination has already seen frames from
     this `(sysid, compid)`, the frame would be echoing back: drop.
   - **Target match** — if the frame carries `target_system` / `target_component`
     (the dialect XML knows which msgids do), RMR sends it only to endpoints
     whose learn-set contains the addressed `(sysid, compid)`. Frames with
     `target_system = 0`, or msgids that have no target field at all, are
     **broadcast** to every surviving destination.
3. Filters (`allow_*` / `block_*`) and sniffer mode are policy knobs layered
   on top of that core. A sniffer endpoint bypasses loop-prevention,
   target-match, and out-filters — it sees every frame the router accepted.

If you only remember three sentences: **RMR learns where each (sysid, compid)
comes from, dispatches targeted messages back to that endpoint, and
broadcasts everything else. Loop-prevention keeps frames from echoing back to
their source. Filters and sniffers are policy on top of that.** The full
flowcharts are under [Filtering & routing decisions](#filtering--routing-decisions).

## CLI cheatsheet

```
rmr [GLOBAL OPTS] ENDPOINT [ENDPOINT ...]

ENDPOINT := SCHEME:BODY[#name][?key=val&...]
```

| Scheme    | Body                          | Use for                          |
|-----------|-------------------------------|----------------------------------|
| `serial:` | `path:baud` (or `path,baud`)  | UART to a flight controller      |
| `udps:`   | `host:port`                   | UDP server (learns many peers)   |
| `udpc:`   | `host:port`                   | UDP client (latches on reply)    |
| `tcps:`   | `host:port`                   | TCP server (accepts many)        |
| `tcpc:`   | `host:port`                   | TCP client (dials + reconnects)  |

Binding `udps:` or `tcps:` to `[::]` is dual-stack on both Linux and
Windows: RMR forces `IPV6_V6ONLY=0` on both platforms for parity, so
v4 peers reach a `[::]`-bound listener identically on each. Without
that, Windows defaults to v6-only and silently drops v4 peers.

Most-used query keys:

| Key                       | Default | Notes                                                |
|---------------------------|---------|------------------------------------------------------|
| `group=NAME`              | —       | Share learn-set across endpoints in `NAME`           |
| `sniffer=true`            | `false` | Diagnostic endpoint; see note below                  |
| `flow_control=rtscts`     | `none`  | `serial:` hardware flow control                      |
| `idle_secs=N`             | 60      | `udps:` peer expiry on inactivity                    |
| `latch_idle_secs=N`       | 30      | `udpc:` revert to configured host after silence      |

A sniffer endpoint sees only frames the router *accepted*. CRC failures,
In-filter rejections (`block_*_in` / `allow_*_in`), and dedup duplicates (when `--dedup-ms > 0` — only the first occurrence reaches the sniffer endpoint) are
all dropped before the sniffer override applies. Capture at the link
layer if you need raw bytes. See
[`examples/simple/fc-sniffer/`](examples/simple/fc-sniffer/config.toml).

Per-endpoint filters are 12 axes (`{allow,block}_{msgid,src_sys,src_comp}_{in,out}`),
each a comma-separated list of decimal integers and inclusive `lo-hi`
ranges — e.g. `block_msgid_in=33,100-150,32`. Hex literals and symbolic
msgid names are intentionally not accepted.

Global options:

```
  -c, --config FILE         TOML config (endpoints + globals)
      --log-level LEVEL     trace|debug|info|warn|error (default info)
      --log-format FMT      text (default) | json
      --skip-config-log     suppress INFO dump of merged config at startup
      --stats               emit per-endpoint stats (JSON-Lines on stdout)
      --stats-interval-secs N   stats output interval (default 5)
      --dedup-ms N          duplicate suppression window (0 = off, default)
  -V, --version             print version (between-release builds also
                            include git short-sha and build date)
```

Full grammar and the complete query-key list are also available at the
terminal via `rmr --help`.

## TOML cheatsheet

Globals match the CLI flags one-for-one (snake_case keys). Endpoints are
an array of tables; the per-endpoint key surface is:

```toml
# globals (all optional)
log_level = "info"             # trace | debug | info | warn | error
log_format = "text"            # text | json
stats = true                   # emit JSON-Lines stats on stdout
stats_interval_secs = 5
dedup_ms = 250                 # default 0 (off); non-zero = TTL in ms.
                               # see examples/advanced/redundant-links/
skip_config_log = false

# one [[endpoints]] table per endpoint
[[endpoints]]
type = "serial"                # serial | udps | udpc | tcps | tcpc
name = "fc"                    # optional; auto-derived from scheme-addr-port
path = "/dev/ttyUSB0"          # serial only
baud = 115200                  # serial only
flow_control = "rtscts"        # serial only; "none" (default) or "rtscts"

[[endpoints]]
type = "udps"
name = "bus"
bind = "0.0.0.0:14550"         # udps / tcps: bind address
idle_secs = 60                 # udps only

[[endpoints]]
type = "tcpc"
host = "192.168.144.15"        # udpc / tcpc: dial target
port = 5760
group = "uplink"               # shared learn-set across same-group endpoints
sniffer = true                 # diagnostic endpoint
# filters are strings; integer-and-range grammar same as CLI
block_msgid_in = "33,100-150"
allow_src_sys_out = "1,5-10"
```

Unknown keys (top-level or per-endpoint) fail at parse time. Per-endpoint
key surface is scheme-validated: a `serial` entry that carries `bind = "..."`
is rejected with a clear error. Filter values must be strings — the array
form is not accepted.

When the same `#name` appears in both TOML and on the CLI, the CLI entry
wholesale replaces the TOML entry and the override is logged at WARN.

## Filtering & routing decisions

Two checkpoints govern every frame: an ingress pipeline at the source
endpoint, then a per-destination check that runs once for every other
endpoint. A frame must pass the source's `*_in` filters AND each
destination's `*_out` filters; the order matches what the code does and
each labelled drop edge maps to a counter in the stats output.

### Ingress — frame received on an endpoint

```
                Frame received on this endpoint
                              │
                              ▼
              ┌──────────────────────────────┐
              │ Known msgid?                 │── no ──┐  (unknown msgid:
              │ (in built-in dialect XML)    │        │   no crc_extra,
              └──────────────┬───────────────┘        │   skip CRC check)
                            yes                       │
                             │                        │
                             ▼                        │
              ┌──────────────────────────────┐        │
              │ CRC matches crc_extra?       │── no ──┼──► DROP
              └──────────────┬───────────────┘        │   crc_errors++
                            yes                       │
                             │                        │
                             └────────────┬───────────┘
                                          │
                                          ▼
                            update rx_lost_est from seq gap
                                          │
                                          ▼
   ┌──────────────────────────────────────────────────┐
   │ For each axis A ∈ {msgid, src_sys, src_comp}:    │
   │   • A not in block_A_in                          │
   │   • allow_A_in empty OR A in allow_A_in          │── no ──► DROP
   └────────────────────────┬─────────────────────────┘     in_filter_drops++
                           yes
                            │
                            ▼
              handed to router (ingress mpsc)
                            │
                            ▼
              ┌──────────────────────────────┐
              │ --dedup-ms > 0 AND xxh3-64   │── yes ──► DROP
              │ already in global window?    │     dedup_drops++
              └──────────────┬───────────────┘
                            no
                             │
                             ▼
                learn (src_sys, src_comp) into
                this endpoint's learn-set
                             │
                             ▼
                evaluate per destination (next tree)
```

Block lists win on overlap: a value listed in both `allow_*_in` and
`block_*_in` is blocked. An empty `allow_*_in` means "no allow
restriction" (all values pass), not "nothing passes."

### Egress — router evaluates each destination D

```
         Frame accepted by router → evaluate destination D
                              │
                              ▼
              ┌──────────────────────────────┐
              │ D is a sniffer               │── yes ──► FORWARD to D
              │ (?sniffer=true)?             │    (bypasses loop-prevent,
              └──────────────┬───────────────┘     out-filter, target match)
                            no
                             │
                             ▼
              ┌──────────────────────────────┐
              │ (src_sys, src_comp) already  │── yes ──► DROP for D
              │ in D's learn-set? (would     │      (loop prevention,
              │ echo back on D's link)       │       silent — no counter)
              └──────────────┬───────────────┘
                            no
                             │
                             ▼
   ┌──────────────────────────────────────────────────┐
   │ For each axis A ∈ {msgid, src_sys, src_comp}:    │
   │   • A not in block_A_out                         │
   │   • allow_A_out empty OR A in allow_A_out        │── no ──► DROP for D
   └────────────────────────┬─────────────────────────┘   out_filter_drops++
                           yes
                            │
                            ▼
              ┌──────────────────────────────┐
              │ Broadcast (target_sys == 0   │
              │ or msgid has no target), OR  │── no ──► DROP for D
              │ target in D's learn-set, OR  │    (target mismatch,
              │ target_comp == 0 with        │     silent — no counter)
              │ target_sys in D's set?       │
              └──────────────┬───────────────┘
                            yes
                             │
                             ▼
                       FORWARD to D
                  (enqueue on D's TxQueue)
```

Same allow/block semantics as ingress. Loop prevention and target
mismatch are silent (no stats counter) — the only counter is
`out_filter_drops`. Unknown msgids carry no target offsets, so they
land at the broadcast branch of the target-match step and reach every
destination that survived loop-prevent and out-filter.

## Stats output

`--stats` emits one JSON-Lines object per endpoint per `--stats-interval-secs`
period (default 5s) on **stdout**. Logs (`tracing`) go to stderr — the two
streams never interleave. Example object for a routable endpoint:

```json
{"ts":"2026-05-15T19:00:00Z","endpoint":"vehicle","state":"connected",
 "rx_frames":12450,"tx_frames":12440,"rx_bytes":2891200,"tx_bytes":2889600,
 "dropped_tx":0,"crc_errors":2,"resync_bytes":7,"rx_lost_est":3,
 "in_filter_drops":0,"out_filter_drops":1,"dedup_drops":15,"learn_entries":4}
```

`tcps:` and `udps:` **parent listeners** emit only `ts`, `endpoint`, and
`state`: they accept connections but no frames traverse them directly
(every accepted client / learned peer is its own routing endpoint), so
the counter fields would all be zero forever. Consumers distinguish the
two shapes by the presence (or absence) of the counter fields.

### Field reference

All counters are **cumulative since process start**; consumers compute
deltas across intervals. The "what it usually means" column gives a
first-look interpretation for an operator — these are heuristics for
triaging the most common conditions, not definitive diagnoses.

| Field              | Meaning                                                                                                                     | What a non-zero value usually means                                                                                                                            |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ts`               | RFC 3339 UTC timestamp of the snapshot, truncated to whole seconds.                                                         | Just a clock — flat-line on `ts` means RMR isn't running.                                                                                                       |
| `endpoint`         | Endpoint name (`#name` or auto-derived). Children of `tcps:` / `udps:` are suffixed `/ip-port`.                              | Identifies the row. Sub-endpoint names are unstable across reconnects (the ephemeral port changes).                                                            |
| `state`            | One of `connected` / `reconnecting` / `idle` / `down`.                                                                       | `reconnecting` for more than one interval = the transport never came up (port held, peer unreachable, device unplugged). `down` is terminal for that endpoint. |
| `rx_frames`        | MAVLink frames *successfully framed and accepted* on this endpoint.                                                          | Non-zero = traffic is flowing in. Flat at 0 = nothing is talking to this endpoint (bad cable, wrong port, peer not configured).                               |
| `tx_frames`        | MAVLink frames written out on this endpoint.                                                                                | Non-zero = the router is forwarding to it. Flat at 0 while sibling endpoints have `rx_frames` rising = filters or loop-prevention are stopping every frame.    |
| `rx_bytes`         | Sum of the framed lengths of every accepted `rx_frames` (resync skips and corrupted frames do not contribute).               | Moves in lockstep with `rx_frames` — useful for link-budget estimates and `bytes_per_frame = rx_bytes / rx_frames`. Wire-level corruption shows up in `resync_bytes` / `crc_errors`, not here. |
| `tx_bytes`         | Bytes written out on the transport.                                                                                          | Pair with `tx_frames` to spot writes blocked at the syscall layer; if `tx_frames` rises but `tx_bytes` is flat, the writer is wedged.                          |
| `dropped_tx`       | Frames evicted from the per-endpoint TX queue. Counts both router-side overflow (`force_push` displaces oldest) and writer-side drain-on-disconnect. | Non-zero = the consumer is slower than the producer (slow link, frozen GCS) or the link recently flapped (drain-on-disconnect). Sustained growth = the link is permanently too slow for the traffic volume. |
| `crc_errors`       | Frames with a known msgid whose CRC did not match the dialect's `crc_extra`.                                                 | **Link integrity** — non-zero indicates wire-level corruption (bad cable, RF interference, EMC on a serial line, mismatched baud). Not signing-related; v2 signatures are forwarded byte-for-byte without validation. |
| `resync_bytes`     | Bytes the framer skipped while looking for the next STX byte after framing failed.                                           | Spikes indicate mid-frame corruption or non-MAVLink data prefixing the stream. Persistent low values are normal at startup (junk in the kernel buffer before the first valid frame). |
| `rx_lost_est`      | Estimated frames lost on the source-side link, computed from MAVLink seq-number gaps under the sanity threshold (64).        | Non-zero = the upstream peer is dropping packets (radio link quality, kernel UDP buffer overrun, serial UART overrun). Best-effort — small gaps under reorder can mis-attribute a few frames. |
| `in_filter_drops`  | Frames the ingress filter (`block_*_in` / `allow_*_in`) refused at this endpoint. Also counts `udpc:` packets dropped for wrong source IP. | Non-zero = either the configured ingress filter is doing its job, or for a `udpc:` endpoint, packets are arriving from an unexpected IP (stale peer, spoofing, misconfigured NAT). Cross-check at DEBUG to tell them apart. |
| `out_filter_drops` | Frames the egress filter (`block_*_out` / `allow_*_out`) refused while evaluating this endpoint as a destination.            | Non-zero = the configured egress filter is doing its job. Compare against ingress traffic on sibling endpoints to estimate how much is being shaped out. |
| `dedup_drops`      | Frames dropped because their xxh3-64 hash matched a recent entry in the global dedup window (`--dedup-ms > 0`).               | Non-zero on a redundant-link deployment = the dedup window is doing what it should. Zero when `--dedup-ms = 0` (default).                                      |
| `learn_entries`    | Number of `(sysid, compid)` entries currently in this endpoint's effective learn-set (capped at 32, LRU-evicted). Members of the same `?group=` share one learn-set, so every group member publishes the **group's** total — not just its own contributions. | Rising from 0 = the endpoint (or its group) is observing new MAVLink sources. Steady = the source population is stable. Saturated at 32 with active turnover (LRU evictions) = a misbehaving source spamming many `(sysid, compid)` pairs, or a deployment that genuinely needs a larger cap. |

**Reading the state field across an outage.** A flapping link looks like
`connected → reconnecting → connected → reconnecting → …`. A peer that
never came up looks like `reconnecting` indefinitely. A `tcps:` accepted
client that disconnected emits one final `down` line then disappears from
subsequent intervals. A `udps:` learned peer that hit its `idle_secs`
threshold emits one final `idle` line then disappears.

**Quick triage cheatsheet.** Three patterns cover most field calls:

- *No traffic at all on an endpoint?* Look at `rx_frames` / `rx_bytes` flat
  at 0 alongside `state: connected` — the transport is up but nothing is
  talking. Check the peer's configuration, cabling, IP routing.
- *Link is up but you see corruption?* `crc_errors` and `resync_bytes`
  both rising — wire-level integrity problem, not a routing problem.
- *Router is dropping frames?* `dropped_tx` rising — slow consumer
  (under-provisioned link), or recent flap that drained the queue. Look
  at the writer-side endpoint's `tx_*` rate vs. the producer-side
  endpoint's `rx_*` rate.

## Custom dialects

RMR ships with `common.xml` + `ardupilotmega.xml` baked into the binary.
Unknown msgids are forwarded as broadcast without CRC validation —
running the upstream binaries against a custom dialect already routes,
just without targeted dispatch or CRC accounting on the custom msgids.

To get targeted routing and CRC validation on a proprietary dialect, fork
the repo, drop the XML into `vendor/mavlink/`, and append the filename to
`DIALECTS` in `build.rs`. Full walkthrough:
[`vendor/mavlink/CUSTOM_DIALECTS.md`](vendor/mavlink/CUSTOM_DIALECTS.md).

## Versioning

Pre-1.0 uses the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility. Cargo treats `0.1` and `0.2` as incompatible —
aligned with this. There is no maintained `0.1.x` patch line once `0.2.0`
ships; development is single-track.

Pin to a fully-qualified version (`:0.1.0`, not `:0.1`) in production.
Docker tags published per release are `:<X.Y.Z>`, `:<X.Y>`, and `:latest`;
no `:<MAJOR>` tag pre-1.0.

Release notes: [CHANGELOG.md](CHANGELOG.md).

## Deployment

Operational deployment patterns — Docker Compose, systemd unit files,
device-access notes — ship under `examples/deployment/` once Phase 8
lands.

## License

Apache-2.0. See [LICENSE](LICENSE).
