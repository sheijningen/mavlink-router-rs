# Vendored MAVLink XML dialect definitions

In-tree copy of the dialect XMLs needed to support `common` and
`ardupilotmega` (the two dialects RMR compiles in by default), plus their
transitive `<include>` dependencies.

## Upstream

- https://github.com/mavlink/mavlink @ `4b3de8fa791c75563af3c20b1bd2e66cbf96d9b8`

## Why a file copy and not a `git submodule`

Keeping the XML in-tree means `build.rs` reads from the local checkout, so
builds need no network access — no submodule fetch, no upstream availability
dependency. Submodules also don't ship with `cargo publish` / `cargo install`
source distributions and add an extra `--recurse-submodules` step to every
fresh clone. The cost of file-copying is under 1 MB.
