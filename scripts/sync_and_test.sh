#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"

src_dir="/mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper"
dst_dir="/home/paanday/rekuiper"

echo "[*] Syncing files to /home/paanday/rekuiper..."
cp "$src_dir/crates/rekuiper-conf/src/lib.rs" "$dst_dir/crates/rekuiper-conf/src/lib.rs"
cp "$src_dir/crates/rekuiper-server/src/lib.rs" "$dst_dir/crates/rekuiper-server/src/lib.rs"
cp "$src_dir/crates/rekuiper-server/src/state.rs" "$dst_dir/crates/rekuiper-server/src/state.rs"
cp "$src_dir/crates/rekuiper-server/src/routes.rs" "$dst_dir/crates/rekuiper-server/src/routes.rs"
cp "$src_dir/crates/rekuiper-server/src/ui.html" "$dst_dir/crates/rekuiper-server/src/ui.html"
cp "$src_dir/crates/rekuiper-server/tests/test_otlp_and_ui.rs" "$dst_dir/crates/rekuiper-server/tests/test_otlp_and_ui.rs"
cp "$src_dir/crates/kuiperd/src/lib.rs" "$dst_dir/crates/kuiperd/src/lib.rs"

cd "$dst_dir"
echo "[*] Running cargo test for test_otlp_and_ui in WSL..."
cargo test -p rekuiper-server --test test_otlp_and_ui --target-dir /home/paanday/target -- test_ui
echo "[*] Cleaning build target for disk hygiene..."
rm -rf /home/paanday/target
echo "[*] Done! Target cleaned."
