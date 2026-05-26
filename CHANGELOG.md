# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0 follows the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility — breaking changes are called out in their own line
under `### Changed` so a reader can spot the break without diffing the code.

## [Unreleased]

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
