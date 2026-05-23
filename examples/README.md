# Examples

Each example is a runnable TOML config. Each example includes the equivalent CLI invocation at the end of the file.

## Simple

1. [`simple/fc-network/`](simple/fc-network/config.toml) — `serial:` reader fanned out to `udpc:`, `udps:`, and `tcps:` endpoints on Linux.
2. [`simple/fc-network-windows/`](simple/fc-network-windows/config.toml) — Same as `fc-network` but with a Windows `COM*` serial path.
3. [`simple/fc-sniffer/`](simple/fc-sniffer/config.toml) — Adds a `?sniffer=true` endpoint with In-filters restricting what the attached recorder may send back onto the bus.
4. [`simple/ipv6-and-hostname/`](simple/ipv6-and-hostname/config.toml) — Binds to IPv6 `[::]`, dials a bracketed v6 literal, and dials a DNS hostname.

## Advanced

5. [`advanced/companion-microservices/`](advanced/companion-microservices/config.toml) — Wire up and route microservices on a companion computer.
6. [`advanced/egress-bandwidth-shaping/`](advanced/egress-bandwidth-shaping/config.toml) — Reduce radio bandwidth usage via filtering.
7. [`advanced/fc-safety-filter/`](advanced/fc-safety-filter/config.toml) — Prevent the GCS from taking manual control; restrict manual control exclusively to the RC controller.
8. [`advanced/fleet-aggregator/`](advanced/fleet-aggregator/config.toml) — Fly a fleet from a single GCS without each drone receiving its siblings' telemetry.
9. [`advanced/ground-side-local-service/`](advanced/ground-side-local-service/config.toml) — Wire up and route microservices on the ground-side.
10. [`advanced/redundant-links/`](advanced/redundant-links/) — Run two parallel links to the same flight controller so a single-link failure is invisible to the GCS.
