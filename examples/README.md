# Examples

Each example is a runnable TOML config. Each example includes the equivalent CLI invocation at the end of the file.

## Simple

Demonstrate the basics of each endpoint type and the most-used query keys.


1. [`simple/fc-network/`](simple/fc-network/config.toml) — `serial:` reader fanned out to `udpc:`, `udps:`, and `tcps:` endpoints on Linux.
2. [`simple/fc-network-windows/`](simple/fc-network-windows/config.toml) — Same as `fc-network` but with a Windows `COM*` serial path.
3. [`simple/fc-sniffer/`](simple/fc-sniffer/config.toml) — Adds a `?sniffer=true` endpoint with In-filters restricting what the attached recorder may send back onto the bus.
4. [`simple/filter-axes/`](simple/filter-axes/config.toml) — All four `{allow,block}_msgid_{in,out}` filter axes side-by-side.
5. [`simple/ipv6-and-hostname/`](simple/ipv6-and-hostname/config.toml) — Binds to IPv6 `[::]`, dials a bracketed v6 literal, and dials a DNS hostname.

## Advanced

Realistic deployment scenarios composing multiple endpoints with routing policy.


6. [`advanced/companion-microservices/`](advanced/companion-microservices/config.toml) — Wire up and route microservices on a companion computer.
7. [`advanced/egress-bandwidth-shaping/`](advanced/egress-bandwidth-shaping/config.toml) — Reduce radio bandwidth usage via filtering.
8. [`advanced/fc-safety-filter/`](advanced/fc-safety-filter/config.toml) — Prevent the GCS from taking manual control; restrict manual control exclusively to the RC controller.
9. [`advanced/fleet-aggregator/`](advanced/fleet-aggregator/config.toml) — Fly a fleet from a single GCS without each drone receiving its siblings' telemetry.
10. [`advanced/ground-side-local-service/`](advanced/ground-side-local-service/config.toml) — Wire up and route microservices on the ground-side.
11. [`advanced/redundant-links/`](advanced/redundant-links/) — Run two parallel links to the same flight controller so a single-link failure is invisible to the GCS.

## Deployment

Ready-to-run manifests for running RMR in production. Both ship the same minimal drone-side `config.toml` (a `serial:` flight controller routed to a `udpc:` GCS).

12. [`deployment/docker-compose/`](deployment/docker-compose/) — Run the multi-arch GHCR image with `network_mode: host` and serial device passthrough.
13. [`deployment/systemd/`](deployment/systemd/) — Run a release binary as a dedicated `rmr` user, with serial access via `DeviceAllow=` and journald logging.
