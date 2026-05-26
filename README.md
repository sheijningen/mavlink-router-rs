# mavlink-router-rs — Rust MAVLink Router

A minimal, high-throughput MAVLink router. Forwards MAVLink traffic
between serial, UDP, and TCP endpoints with a learned routing table that
improves targeted delivery over pure broadcast. Bytes in, bytes out —
RMR owns no MAVLink identity, never emits a message of its own, and
parses no more of each message than it needs to in order to route it.

RMR implements **no GCS features**: it does not emit `HEARTBEAT`s,
never sends `REQUEST_DATA_STREAM` or `SET_MESSAGE_INTERVAL`, and
originates no traffic of its own. Per-endpoint filters enable highly customizable
forwarding behavior between endpoints. ("RMR" is the short-form alias
used throughout the docs; the binary is also called `rmr`.)

## Install

### Prebuilt binaries

Each release attaches binaries for Linux (`x86_64` and `aarch64`, both
`musl`-static and dynamically-linked `gnu`), macOS (`x86_64` and `aarch64`),
and Windows (`x86_64`) to the GitHub Release. Verify with the attached
`SHA256SUMS`. For a long-running service, see the
[systemd manifest](examples/deployment/systemd/).

### Docker

Multi-arch images (`linux/amd64`, `linux/arm64`) are published per
release to `ghcr.io/sheijningen/rmr` with tags `:<X.Y.Z>` and `:latest`.
**Pin to `:<X.Y.Z>` in production**. (Image and binary share the
short name; the longer `mavlink-router-rs` lives at the GitHub repo and
README title for discoverability.)

```sh
docker run --rm -i -p 14550:14550/udp --device /dev/ttyAMA0 \
  ghcr.io/sheijningen/rmr:latest \
  serial:/dev/ttyAMA0:115200#fc udps:0.0.0.0:14550#gcs
```

For a config-file driven setup, see the
[Docker Compose manifest](examples/deployment/docker-compose/).

## Quick start

Expose a flight controller to the network over TCP:

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
accepted message to every other endpoint — skipping a destination on
loop-prevention (it has already seen the source), `*_out` filter
rejection, or target mismatch (the message's `(target_sys,
target_comp)` isn't in the destination's learn-set; broadcast traffic
skips this check). Source endpoints muzzle their own ingress via
`*_in` filters. A `?sniffer=true` destination skips the three
per-destination checks and sees all accepted traffic.

For server schemes, **each connected peer is its own routing
endpoint**: a `tcps:` listener spawns one per accepted client, a
`udps:` socket spawns one per learned source address. Each child has
its own learn-set and stats; filters and `?group=` on the parent
apply to every child by reference.

See [Routing pipeline](#routing-pipeline) for per-step semantics.

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
| `sniffer=true`            | `false` | Diagnostic endpoint (sees all accepted traffic)      |
| `flow_control=rtscts`     | `none`  | `serial:` hardware flow control                      |
| `idle_secs=N`             | 60      | `udps:` peer expiry on inactivity                    |
| `latch_idle_secs=N`       | 30      | `udpc:` revert to configured host after silence      |

Per-endpoint filters are 12 axes
(`{allow,block}_{msgid,src_sys,src_comp}_{in,out}`), each a
comma-separated list of decimal integers and inclusive `lo-hi` ranges —
e.g. `block_msgid_in=33,100-150,32`. Hex literals and symbolic msgid
names are intentionally not accepted.

> **Filter direction (`_in` vs `_out`).**
>
> - `_in` filters reject frames arriving from the wire *into* the
>   endpoint. Rejection drops the frame for every destination.
> - `_out` filters reject frames the router is about to send *out* on
>   the wire from the endpoint. One rejection only suppresses
>   delivery through that endpoint.
>
> A frame reaches a destination only if it passes the source's `_in`
> and that destination's `_out`.
>
> See [`examples/simple/filter-axes/`](examples/simple/filter-axes/config.toml)
> for a worked example with all four `{allow,block}_msgid_{in,out}`
> axes side-by-side.

Globals:

```
  -c, --config FILE         TOML config (endpoints + globals)
      --log-level LEVEL     trace|debug|info|warn|error (default info)
      --log-format FMT      text (default) | json
      --skip-config-log     suppress INFO dump of merged config at startup
      --stats               emit per-endpoint stats (JSON-Lines on stdout)
      --stats-interval-secs N   stats output interval (default 5)
      --dedup-ms N          duplicate suppression window (0 = off, default)
      --dry-run             print the merged config to stdout and exit
                            without binding any sockets
  -V, --version             print version
```

Full grammar and the complete query-key list are also available at the
terminal via `rmr --help`.

## TOML

A TOML config file is the recommended way to set up RMR for anything
beyond a quick one-shot test: the CLI covers the same surface but does
not scale to multi-endpoint setups with filters and globals.

Globals match the CLI flags one-for-one (snake_case keys). Endpoints are
an array of tables:

```toml
# globals (all optional)
log_level = "info"             # trace | debug | info | warn | error
log_format = "text"            # text | json
stats = true                   # emit JSON-Lines stats on stdout
stats_interval_secs = 5
dedup_ms = 2000                # default 0 (off); non-zero = TTL in ms.
                               # Err high — too low misses duplicates,
                               # too high is essentially free.
                               # See examples/advanced/redundant-links/
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

**Config validation and CLI/TOML merge rules:**

- **Validation is strict.** Any unknown key — top-level or
  per-endpoint — is fatal at startup.
- **Endpoint names must be unique within each source.** A duplicate
  `#name` inside the CLI, or inside the TOML, is fatal.
- **CLI overrides TOML, per key.** For globals, any CLI flag that is
  set overrides the matching TOML key.
- **CLI overrides TOML, per endpoint.** When the same endpoint `#name` appears
  in both TOML and on the CLI, the CLI entry wholesale replaces the
  TOML entry.

## Reconnects

Bind or open failure at startup is **not fatal**: the affected endpoint
enters its retry loop (`state = reconnecting`, WARN per attempt) and
the router comes up. Same loop handles loss-of-resource at runtime.

## Routing pipeline

Every frame traverses two checkpoints: an ingress pipeline at the
source endpoint, then a per-destination check that runs once for each
other endpoint.

**Ingress** (source endpoint):

1. Validate CRC against the msgid's `crc_extra`. A known msgid with a
   bad CRC is **dropped**; unknown msgids forward without CRC
   validation.
2. Apply `allow_*_in` / `block_*_in` filters across
   `{msgid, src_sys, src_comp}`. A rejected frame is **dropped** and
   never reaches the router.
3. If `--dedup-ms > 0` and a recent identical frame is still in the
   global dedup window, the duplicate is **dropped**.
4. Learn `(src_sys, src_comp)` into this endpoint's learn-set. The
   frame **proceeds** to the per-destination check.

**Per-destination D** (runs for every other endpoint):

1. If D is `?sniffer=true`: **forward** (skip remaining checks).
2. If D's learn-set already contains `(src_sys, src_comp)`: **drop**
   for D (loop-prevention).
3. Apply `allow_*_out` / `block_*_out` filters. A rejected frame is
   **dropped** for D; other destinations are unaffected.
4. **Drop** for D if the message is targeted and its
   `(target_sys, target_comp)` is not in D's learn-set. Otherwise
   **forward** to D (broadcast frames, and targeted frames whose
   addressee D has learned, both fall through here).

Block lists win on overlap: a value listed in both `allow_*` and
`block_*` is blocked. An empty `allow_*` means "no allow restriction"
(all values pass), not "nothing passes." Unknown msgids carry no
target offsets and always land at the broadcast branch of step 4.

## Stats

`--stats` emits one JSON-Lines object per endpoint per
`--stats-interval-secs` (default 5s) on **stdout**. Logs (`tracing`) go
to stderr — the two streams never interleave.

The schema doubles as a debug guide: when traffic isn't reaching where
you expect, the counter that did (or didn't) move points at the stage
that rejected it. The "Non-zero indicates" column below is meant to be
read in that mode.

`tcps:` and `udps:` listeners emit only `ts`, `endpoint`, and `state`
— their accepted clients and learned peers each emit the per-connection
shape below.

Per-connection endpoint:

```json
{"ts":"2026-05-15T19:00:00Z","endpoint":"vehicle","state":"connected",
 "rx_frames":12450,"tx_frames":12440,"rx_bytes":2891200,"tx_bytes":2889600,
 "dropped_tx":0,"crc_errors":2,"resync_bytes":7,"rx_lost_est":3,
 "in_filter_drops":0,"out_filter_drops":1,"dedup_drops":15,"learn_entries":4}
```

All counters are cumulative since process start; consumers compute
deltas.

| Field              | Meaning                                                                                        | Non-zero indicates                                          |
|--------------------|------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `ts`               | RFC 3339 UTC timestamp.                                                                        | —                                                            |
| `endpoint`         | Endpoint name. Sub-endpoints suffixed `/ip-port` (unstable across reconnects).                 | —                                                            |
| `state`            | `connected` / `reconnecting` / `idle` / `down`.                                                | Transport state.                                             |
| `rx_frames`        | Frames accepted on this endpoint.                                                            | Inbound traffic is flowing.                                  |
| `tx_frames`        | Frames written out on this endpoint.                                                         | The router is forwarding to it.                              |
| `rx_bytes`         | Bytes accepted on this endpoint.                                                   | Pair with `rx_frames` for bytes-per-frame.                 |
| `tx_bytes`         | Bytes written out on this endpoint.                                                                             | Pair with `tx_frames` to spot writes blocked at the syscall. |
| `dropped_tx`       | TX-queue evictions: router-side overflow + writer-side drain-on-disconnect.                    | Slow consumer or recent link flap.                           |
| `crc_errors`       | Known-msgid frames whose CRC didn't match `crc_extra`.                                       | Wire-level corruption (cable, RF, baud mismatch).            |
| `resync_bytes`     | Bytes skipped while resynchronizing to the next message boundary.                              | Mid-message corruption or non-MAVLink prefix bytes.          |
| `rx_lost_est`      | Estimated lost frames from seq-number gaps.                                            | Upstream packet loss (radio, UDP buffer, UART overrun).      |
| `in_filter_drops`  | Frames refused by `allow_*_in` / `block_*_in`. Also `udpc:` packets dropped for wrong source IP. | Configured ingress filter rejecting traffic, or stale peer.  |
| `out_filter_drops` | Frames refused by `allow_*_out` / `block_*_out` while evaluating this endpoint as destination. | Configured egress filter rejecting traffic.                    |
| `dedup_drops`      | Frames suppressed as duplicates by `--dedup-ms`.                                             | `--dedup-ms > 0` is suppressing duplicates.                  |
| `learn_entries`    | Current `(sysid, compid)` entries in this endpoint's (or group's) learn-set.                   | Observing MAVLink sources.                                   |

## Development

End-user installs use the prebuilt binaries and Docker images attached
to each release (see [Install](#install)). Building from source is for
contributors and integrators carrying patches; RMR is not published to
crates.io.

### Building from source

Rust Edition 2024, MSRV `1.88`.

```sh
git clone https://github.com/sheijningen/mavlink-router-rs
cd mavlink-router-rs
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
cargo +nightly fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

## Versioning

Pre-1.0 uses the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility. Cargo treats `0.1` and `0.2` as incompatible.
There is no maintained `0.1.x` patch line once `0.2.0` ships;
development is single-track.

Pin to a fully-qualified version (`:0.1.0`) in production. Docker tags
published per release: `:<X.Y.Z>` and `:latest`.

Release notes: [CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
