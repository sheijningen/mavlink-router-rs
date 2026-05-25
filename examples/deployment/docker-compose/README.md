# RMR via Docker Compose

Runs RMR from the multi-arch docker image.

## One-time setup

1. **Pick an image tag**. The shipped `docker-compose.yml` uses
   `:latest` for convenience. In production, pin to a specific release
   (`:0.1.0`, `:0.1`).

2. **Adapt the manifest and config to your host**:

   - **Endpoints.** Replace the shipped endpoints in `config.toml`
     with the ones that fit your use case.
   - **Serial device.** Update `path` in `config.toml` and `devices`
     in `docker-compose.yml` together.

3. **Bring it up**:

   ```sh
   docker compose up -d
   docker compose logs -f rmr
   ```

## Running without `network_mode: host`

`network_mode: host` is the default because it gives RMR the same
view of the network the binary would have running on the host
directly — which is what every RMR routing primitive assumes. If
you decide to run RMR namespaced from the host instead, swap it for
an explicit `ports:` block listing every endpoint the host needs to
reach:

```yaml
services:
  rmr:
    image: ghcr.io/sheijningen/rmr:latest
    command: ["-c", "/etc/rmr/config.toml"]

    ports:
      - "5760:5760/tcp"      # tcps: listener
      - "14550:14550/udp"    # udps: listener

    devices:
      - "/dev/ttyAMA0:/dev/ttyAMA0"
    volumes:
      - ./config.toml:/etc/rmr/config.toml:ro
    restart: unless-stopped
```

Every `tcps:` and `udps:` bind in `config.toml` needs a matching
`ports:` entry; outbound `tcpc:` and `udpc:` need no mapping.
