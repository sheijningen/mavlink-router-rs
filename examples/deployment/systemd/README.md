# RMR via systemd

Runs RMR as a system service from a native binary install.

## One-time setup

1. **Install the binary**. Download the binary matching your host from
   the [GitHub Release](https://github.com/sheijningen/mavlink-router-rs/releases)
   page, verify against the release's `SHA256SUMS`, and place it at
   `/usr/local/bin/rmr` (the path `ExecStart=` uses).

2. **Adapt the unit and config to your host**:

   - **Endpoints.** Replace the shipped endpoints in `config.toml`
     with the ones that fit your use case.
   - **Serial device.** Update `path` in `config.toml` and
     `DeviceAllow` in `rmr.service` together.
   - **Serial access group.** `SupplementaryGroups=dialout` works on
     Debian/Ubuntu/Fedora/RHEL; change to `uucp` on Arch/openSUSE.

3. **Provision the service**:

   ```sh
   set -euo pipefail

   # Create the dedicated system account.
   sudo useradd --system --shell /usr/sbin/nologin rmr

   # Install the config and unit.
   sudo install -d /etc/rmr
   sudo install -m 0644 config.toml /etc/rmr/config.toml
   sudo install -m 0644 rmr.service /etc/systemd/system/rmr.service
   sudo systemctl daemon-reload

   # Enable and start.
   sudo systemctl enable --now rmr.service
   sudo journalctl -u rmr -f
   ```
