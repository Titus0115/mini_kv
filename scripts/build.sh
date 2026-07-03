#!/usr/bin/env bash
set -euo pipefail
BINARY="$1"
DIST_DIR="${2:-./dist}"
mkdir -p "$DIST_DIR"
cargo build --release --bin "$BINARY"
cp "target/release/$BINARY" "$DIST_DIR/"
echo "Built $BINARY -> $DIST_DIR/$BINARY"