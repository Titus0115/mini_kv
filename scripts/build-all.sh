#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DIST_DIR="${1:-$PROJECT_DIR/dist}"
mkdir -p "$DIST_DIR"
for bin in mini-kv-server mini-kv-cli; do
  cargo build --release --bin "$bin"
  cp "target/release/$bin" "$DIST_DIR/"
  echo "Built $bin -> $DIST_DIR/$bin"
done