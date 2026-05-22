# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0 follows the SemVer "0.x is unstable" carve-out: `0.x.y → 0.(x+1).0`
may break compatibility — breaking changes are called out in their own line
under `### Changed` so a reader can spot the break without diffing the code.

## [Unreleased]

### Added

- Endpoints: `serial:`, `udps:`, `udpc:`, `tcps:`, `tcpc:`. Cross-platform
  (Linux, macOS, Windows). IPv4 + IPv6, dual-stack on `[::]`.
- Routing pipeline: per-source seq-loss accounting, ingress dedup,
  learn-table-driven target match, loop prevention, sniffer override.
- Per-endpoint filters on 12 axes
  (`{allow,block}_{msgid,src_sys,src_comp}_{in,out}`).
- Endpoint groups via `?group=NAME` — members share a learn-set; filters
  and stats remain per-endpoint.
- Optional global dedup window (`--dedup-ms` / `dedup_ms`, default off);
  catches redundant-uplink duplicates across ingress endpoints.
- Periodic per-endpoint stats output (JSON-Lines on stdout) with two
  shapes — full counter schema for routable endpoints and a state-only
  shape for parent listeners.
- TOML configuration matching the CLI surface plus filters and groups;
  CLI > TOML > defaults precedence on globals, CLI wholesale-replace on
  same-name endpoints with a WARN.
- Compile-time MAVLink dialect table covering `common.xml` +
  `ardupilotmega.xml`. Unknown msgids forward as broadcast without CRC
  validation; custom dialects via fork
  (see [`vendor/mavlink/CUSTOM_DIALECTS.md`](vendor/mavlink/CUSTOM_DIALECTS.md)).
- Endpoint mini-guide in `rmr --help` (`SCHEME:BODY[#name][?key=val&...]`
  grammar plus one canonical example per scheme).
- Hot-replug recovery on `serial:` endpoints — a device that disappears
  mid-stream is reopened automatically without restarting the router
  (an improvement over `mavlink-router`).
- `--skip-config-log` to suppress the INFO dump of the resolved config
  at startup, for setups where the config is sensitive or noisy.
