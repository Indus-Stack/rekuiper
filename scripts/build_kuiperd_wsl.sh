#!/bin/bash
set -euo pipefail

echo "=== Rekuiper Build in WSL ==="
export PATH="$HOME/.cargo/bin:$PATH"
echo "Rust version:"
rustc --version
cargo --version

mkdir -p /home/paanday/fleet/bin
mkdir -p /tmp/cargo-target

echo "Building kuiperd in release mode..."
cd /home/paanday/rekuiper
cargo build --release -p kuiperd --target-dir /tmp/cargo-target

echo "Copying binary to /home/paanday/fleet/bin/kuiperd..."
cp /tmp/cargo-target/release/kuiperd /home/paanday/fleet/bin/kuiperd
chmod +x /home/paanday/fleet/bin/kuiperd

echo "Cleaning build cache in /tmp/cargo-target..."
rm -rf /tmp/cargo-target

echo "Binary ready:"
ls -lh /home/paanday/fleet/bin/kuiperd
