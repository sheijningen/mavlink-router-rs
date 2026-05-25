#!/usr/bin/env bash
#
# Run the same checks CI runs, in the same order. Fails fast on the first error.
#
# Usage: scripts/check.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

cargo +nightly fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
