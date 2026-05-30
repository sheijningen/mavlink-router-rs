# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0 follows the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility — breaking changes are called out in their own line
under `### Changed` so a reader can spot the break without diffing the code.

## [Unreleased]

### Added

- Egress filter axis keyed on the source endpoint's name:
  `allow_src_endpoint_out` / `block_src_endpoint_out`. Controls which
  endpoints' traffic a destination receives without listing every other
  axis. Clients of a `udps:`/`tcps:` server share that server's name and
  are matched as one source.
- Config validation: filters referencing a non-existent endpoint name
  are now fatal at startup.
- Windows shutdown signal handling for `CTRL_CLOSE`, `CTRL_SHUTDOWN`,
  and `CTRL_BREAK` (previously only `CTRL_C`).

### Changed

- **Breaking (config):** TOML endpoints are now `[endpoint.NAME]` tables
  instead of an `[[endpoints]]` array with a `name` field. The table key
  is the endpoint name, so names are unique by construction and a
  duplicate is a TOML parse error. Existing configs must be migrated.
- Endpoint names are no longer capped at 64 characters.

### Fixed

- Reject unbracketed IPv6 hosts in `host:port` specs instead of
  misparsing them.
- Back off on sustained socket errors uniformly: the shared retry delay
  now also applies to the UDP receive loop and the TCP accept loop,
  preventing hot loops.
- Ignore stale `WSAECONNRESET` on Windows UDP sockets.
- Prevent startup panic when `stats_interval_secs = 0`.
- Reap child tasks of TCP/UDP servers on shutdown instead of leaking
  their join handles.

## [0.1.0] - 2026-05-26

### Added

- Endpoints: `serial:`, `udps:`, `udpc:`, `tcps:`, `tcpc:`. Cross-platform
  (Linux, macOS, Windows), IPv4 + IPv6.
- Routing pipeline: per-source seq-loss accounting, ingress dedup,
  learn-table-driven target match, loop prevention, sniffer override.
- Per-endpoint filters on 12 axes
  (`{allow,block}_{msgid,src_sys,src_comp}_{in,out}`).
- Endpoint groups via `?group=NAME` — members share a learn-set; filters
  and stats remain per-endpoint.
- Optional global dedup window (`--dedup-ms` / `dedup_ms`, default off).
- Periodic per-endpoint stats output (JSON-Lines on stdout).
- TOML configuration matching the CLI surface plus filters and groups.
- Compile-time MAVLink dialect table covering `common.xml` +
  `ardupilotmega.xml`; custom dialects via fork
  (see [`vendor/mavlink/CUSTOM_DIALECTS.md`](vendor/mavlink/CUSTOM_DIALECTS.md)).
- Endpoint mini-guide in `rmr --help`.
- Hot-replug recovery on `serial:` endpoints.
- `--skip-config-log` to suppress the INFO dump of the resolved config
  at startup.
- Distribution: prebuilt binaries and multi-arch Docker images
  (`ghcr.io/sheijningen/rmr`); runnable example configs under
  [`examples/`](examples/); Docker Compose and systemd deployment
  manifests under [`examples/deployment/`](examples/deployment/).
