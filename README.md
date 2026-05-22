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
  udps:0.0.0.0:14550#bus tcpc:gcs.local:5760#vehicle
```

`--network host` is required for `udps:` listeners learning peers from
arbitrary source addresses; bridge networking with `-p` mappings works
for unicast-only setups. Pass serial devices through with
`--device=/dev/ttyUSB0:/dev/ttyUSB0`.

## Quick start

Route between a flight-controller serial link and a GCS over TCP:

```sh
rmr serial:/dev/ttyUSB0:921600#fc tcps:0.0.0.0:5760#gcs
```

Same setup driven from a TOML file:

```toml
[[endpoints]]
type = "serial"
name = "fc"
path = "/dev/ttyUSB0"
baud = 921600

[[endpoints]]
type = "tcps"
name = "gcs"
bind = "0.0.0.0:5760"
```

```sh
rmr -c rmr.toml
```

More worked topologies — redundant uplinks, multi-drone aggregation,
sniffer taps, NAT traversal — under [`examples/`](examples/).

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

Most-used query keys:

| Key                       | Default | Notes                                                |
|---------------------------|---------|------------------------------------------------------|
| `group=NAME`              | —       | Share learn-set across endpoints in `NAME`           |
| `sniffer=true`            | `false` | Receive every routed frame (tap / logger)            |
| `flow_control=rtscts`     | `none`  | `serial:` hardware flow control                      |
| `idle_secs=N`             | 60      | `udps:` peer expiry on inactivity                    |
| `latch_idle_secs=N`       | 30      | `udpc:` revert to configured host after silence      |

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
dedup_ms = 250                 # default 0 (off); non-zero = TTL in ms
skip_config_log = false

# one [[endpoints]] table per endpoint
[[endpoints]]
type = "serial"                # serial | udps | udpc | tcps | tcpc
name = "fc"                    # optional; auto-derived from scheme-addr-port
path = "/dev/ttyUSB0"          # serial only
baud = 921600                  # serial only
flow_control = "rtscts"        # serial only; "none" (default) or "rtscts"

[[endpoints]]
type = "udps"
name = "bus"
bind = "0.0.0.0:14550"         # udps / tcps: bind address
idle_secs = 60                 # udps only

[[endpoints]]
type = "tcpc"
host = "gcs.local"             # udpc / tcpc: dial target
port = 5760
group = "uplink"               # shared learn-set across same-group endpoints
sniffer = true                 # diagnostic tap
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
