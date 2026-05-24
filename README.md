# RMR — Rust MAVLink Router

A minimal, high-throughput MAVLink router. Forwards MAVLink frames between
serial, UDP, and TCP endpoints with a learned routing table that improves
targeted delivery over pure broadcast. Bytes in, bytes out — RMR owns no
MAVLink identity, never emits a message of its own, and parses no more of a frame than it needs to in order to route it.

## Install

### Prebuilt binaries

Each release attaches binaries for Linux (`x86_64` and `aarch64`, both
`musl`-static and dynamically-linked `gnu`), macOS (`x86_64` and `aarch64`),
and Windows (`x86_64`) to the GitHub Release. Verify with the attached
`SHA256SUMS`.

### Docker

Multi-arch images (`linux/amd64`, `linux/arm64`) are published per
release to `ghcr.io/sheijningen/rmr` with tags `:<X.Y.Z>`, `:<X.Y>`, and
`:latest`. Pin to `:<X.Y.Z>` in production.

```sh
docker run --rm -i -p 14550:14550/udp --device /dev/ttyAMA0 \
  ghcr.io/sheijningen/rmr:0.1.0 \
  serial:/dev/ttyAMA0:115200#fc udps:0.0.0.0:14550#gcs
```

## Quick start

Forward between a flight controller and a GCS over TCP:

```sh
rmr serial:/dev/ttyUSB0:115200#fc tcps:0.0.0.0:5760#gcs
```

Same setup from a TOML file:

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

More topologies — multi-GCS fan-out, redundant uplinks, multi-drone
aggregation, safety filters — under [`examples/`](examples/README.md).

## How it works

RMR has no MAVLink identity. It learns which endpoint each `(sysid,
compid)` lives behind by watching traffic, then forwards every
accepted frame to every other endpoint — skipping a destination on
loop-prevention (it has already seen the source), `*_out` filter
rejection, or target mismatch (the frame's `(target_sys, target_comp)`
isn't in the destination's learn-set; broadcast frames skip this
check). Source endpoints muzzle their own ingress via `*_in` filters.
A `?sniffer=true` destination skips the three per-destination checks
and sees every frame the router accepts.

See [Routing pipeline](#routing-pipeline) for per-step semantics and
the stats counter each drop maps to.

## CLI

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

Example:

```sh
rmr tcps:0.0.0.0:5760#vehicle?block_msgid_in=33,100-150
```

Binding `udps:` or `tcps:` to `[::]` is dual-stack on Linux and Windows
alike — RMR forces `IPV6_V6ONLY=0` on both so v4 peers reach a
`[::]`-bound listener identically on each.

Most-used query keys:

| Key                       | Default | Notes                                                |
|---------------------------|---------|------------------------------------------------------|
| `group=NAME`              | —       | Share learn-set across endpoints in `NAME`           |
| `sniffer=true`            | `false` | Diagnostic endpoint (sees every accepted frame)      |
| `flow_control=rtscts`     | `none`  | `serial:` hardware flow control                      |
| `idle_secs=N`             | 60      | `udps:` peer expiry on inactivity                    |
| `latch_idle_secs=N`       | 30      | `udpc:` revert to configured host after silence      |

Per-endpoint filters are 12 axes
(`{allow,block}_{msgid,src_sys,src_comp}_{in,out}`), each a
comma-separated list of decimal integers and inclusive `lo-hi` ranges —
e.g. `block_msgid_in=33,100-150,32`. Hex literals and symbolic msgid
names are intentionally not accepted.

Globals:

```
  -c, --config FILE         TOML config (endpoints + globals)
      --log-level LEVEL     trace|debug|info|warn|error (default info)
      --log-format FMT      text (default) | json
      --skip-config-log     suppress INFO dump of merged config at startup
      --stats               emit per-endpoint stats (JSON-Lines on stdout)
      --stats-interval-secs N   stats output interval (default 5)
      --dedup-ms N          duplicate suppression window (0 = off, default)
  -V, --version             print version
```

Full grammar and the complete query-key list are also available at the
terminal via `rmr --help`.

## TOML

Globals match the CLI flags one-for-one (snake_case keys). Endpoints are
an array of tables:

```toml
# globals (all optional)
log_level = "info"             # trace | debug | info | warn | error
log_format = "text"            # text | json
stats = true                   # emit JSON-Lines stats on stdout
stats_interval_secs = 5
dedup_ms = 250                 # default 0 (off); non-zero = TTL in ms
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
keys are scheme-validated — a `serial` entry that carries `bind = "..."`
is rejected with a clear error. Filter values must be strings; the array
form is not accepted.

When the same `#name` appears in both TOML and on the CLI, the CLI
entry wholesale replaces the TOML entry and the override is logged at
WARN.

## Routing pipeline

Every frame traverses two checkpoints: an ingress pipeline at the
source endpoint, then a per-destination check that runs once for each
other endpoint. Each labelled drop maps to a stats counter.

**Ingress** (source endpoint):

1. Validate CRC against the msgid's `crc_extra` (known msgids only;
   unknown msgids forward without CRC validation). Mismatch →
   `crc_errors++`.
2. Update `rx_lost_est` from the MAVLink seq-number gap.
3. Apply `allow_*_in` / `block_*_in` filters across
   `{msgid, src_sys, src_comp}`. Reject → `in_filter_drops++`.
4. Hand to the router. If `--dedup-ms > 0` and the xxh3-64 hash is
   already in the global dedup window: drop, `dedup_drops++`.
5. Learn `(src_sys, src_comp)` into this endpoint's learn-set.

**Per-destination D** (runs for every other endpoint):

1. If D is `?sniffer=true`: forward (skip remaining checks).
2. If D's learn-set already contains `(src_sys, src_comp)`: drop
   (loop-prevention; silent — no counter).
3. Apply `allow_*_out` / `block_*_out` filters. Reject →
   `out_filter_drops++`.
4. Accept if the frame is broadcast (`target_sys == 0`, or the msgid
   has no target field), or `(target_sys, target_comp)` is in D's
   learn-set, or `target_comp == 0` and `target_sys` is in D's set.
   Otherwise drop (target mismatch; silent — no counter).
5. Enqueue on D's TxQueue.

Block lists win on overlap: a value listed in both `allow_*` and
`block_*` is blocked. An empty `allow_*` means "no allow restriction"
(all values pass), not "nothing passes." Unknown msgids carry no
target offsets and always land at the broadcast branch of step 4.

## Stats

`--stats` emits one JSON-Lines object per endpoint per
`--stats-interval-secs` (default 5s) on **stdout**. Logs (`tracing`) go
to stderr — the two streams never interleave.

Routable endpoint:

```json
{"ts":"2026-05-15T19:00:00Z","endpoint":"vehicle","state":"connected",
 "rx_frames":12450,"tx_frames":12440,"rx_bytes":2891200,"tx_bytes":2889600,
 "dropped_tx":0,"crc_errors":2,"resync_bytes":7,"rx_lost_est":3,
 "in_filter_drops":0,"out_filter_drops":1,"dedup_drops":15,"learn_entries":4}
```

`tcps:` and `udps:` **parent listeners** emit only `ts`, `endpoint`,
and `state` — no frames traverse them directly (every accepted client
or learned peer is its own routing endpoint). Consumers distinguish
the two shapes by the presence of counter fields.

All counters are cumulative since process start; consumers compute
deltas.

| Field              | Meaning                                                                                        | Non-zero indicates                                          |
|--------------------|------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `ts`               | RFC 3339 UTC timestamp.                                                                        | —                                                            |
| `endpoint`         | Endpoint name. Sub-endpoints suffixed `/ip-port` (unstable across reconnects).                 | —                                                            |
| `state`            | `connected` / `reconnecting` / `idle` / `down`.                                                | Transport state.                                             |
| `rx_frames`        | Frames framed and accepted on this endpoint.                                                   | Inbound traffic is flowing.                                  |
| `tx_frames`        | Frames written out on this endpoint.                                                           | The router is forwarding to it.                              |
| `rx_bytes`         | Sum of framed lengths of every accepted `rx_frames`.                                           | Pair with `rx_frames` for bytes-per-frame.                   |
| `tx_bytes`         | Bytes written out.                                                                             | Pair with `tx_frames` to spot writes blocked at the syscall. |
| `dropped_tx`       | TX-queue evictions: router-side overflow + writer-side drain-on-disconnect.                    | Slow consumer or recent link flap.                           |
| `crc_errors`       | Known-msgid frames whose CRC didn't match `crc_extra`.                                         | Wire-level corruption (cable, RF, baud mismatch).            |
| `resync_bytes`     | Bytes the framer skipped scanning for the next STX.                                            | Mid-frame corruption or non-MAVLink prefix bytes.            |
| `rx_lost_est`      | Estimated lost frames from seq-number gaps under the sanity threshold (64).                    | Upstream packet loss (radio, UDP buffer, UART overrun).      |
| `in_filter_drops`  | Frames refused by `allow_*_in` / `block_*_in`. Also `udpc:` packets dropped for wrong source IP. | Configured ingress filter rejecting traffic, or stale peer.  |
| `out_filter_drops` | Frames refused by `allow_*_out` / `block_*_out` while evaluating this endpoint as destination. | Configured egress filter shaping traffic.                    |
| `dedup_drops`      | Frames whose hash matched a live entry in the global dedup window.                             | `--dedup-ms > 0` is suppressing duplicates.                  |
| `learn_entries`    | Current `(sysid, compid)` entries in this endpoint's (or group's) learn-set. Cap 32, LRU-evicted. | Observing MAVLink sources.                                   |

## Development

End-user installs use the prebuilt binaries and Docker images attached
to each release (see [Install](#install)). Building from source is for
contributors and integrators carrying patches; RMR is not published to
crates.io.

### Building from source

Edition 2024, MSRV `1.85`.

```sh
git clone https://github.com/sheijningen/rmr
cd rmr
cargo build --release
# binary at target/release/rmr
```

### Adding custom dialects

RMR ships with `common.xml` + `ardupilotmega.xml` baked into the binary.
Unknown msgids forward as broadcast without CRC validation — upstream
binaries route custom dialects already, just without targeted dispatch
or CRC accounting on the custom msgids.

For targeted routing and CRC validation on a proprietary dialect, fork
the repo, drop the XML into `vendor/mavlink/`, and append the filename
to `DIALECTS` in `build.rs`. Full walkthrough:
[`vendor/mavlink/CUSTOM_DIALECTS.md`](vendor/mavlink/CUSTOM_DIALECTS.md).

### Contributing

CI runs three checks on every push; all three must pass locally
before pushing:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Design rationale, locked architectural decisions, and the phased
roadmap live in [CLAUDE.md](CLAUDE.md). Read it before changing core
behavior; update it when the design changes.

## Versioning

Pre-1.0 uses the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility. Cargo treats `0.1` and `0.2` as incompatible.
There is no maintained `0.1.x` patch line once `0.2.0` ships;
development is single-track.

Pin to a fully-qualified version (`:0.1.0`, not `:0.1`) in production.
Docker tags published per release: `:<X.Y.Z>`, `:<X.Y>`, and `:latest`
(no `:<MAJOR>` tag pre-1.0).

Release notes: [CHANGELOG.md](CHANGELOG.md).

## Deployment

Operational deployment patterns — Docker Compose, systemd unit files,
device-access notes — ship under `examples/deployment/` once Phase 8
lands.

## License

Apache-2.0. See [LICENSE](LICENSE).
