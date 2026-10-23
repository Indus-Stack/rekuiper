#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-C debuginfo=0 -C strip=symbols"

SRC="/mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper"
DST="/home/paanday/rekuiper"

echo "=== [1/5] Syncing latest repo files to WSL ==="
rsync -av --exclude "target" --exclude "node_modules" --exclude ".vitepress/dist" --exclude ".git" "$SRC/" "$DST/"

cd "$DST"

echo "=== [2/5] Running cargo fmt check ==="
cargo fmt --all -- --check || (echo "Formatting files..." && cargo fmt --all && rsync -av --include="*/" --include="*.rs" --exclude="*" "$DST/crates/" "$SRC/crates/")

echo "=== [3/5] Checking Core Crates Zero-Ignore Isolation & RustSec Advisories ==="
cargo deny check advisories
if cargo tree -p rekuiper-core -p rekuiper-sql -i rustls@0.21.12 2>/dev/null; then
  echo "ERROR: rustls 0.21.12 found in core crates"
  exit 1
fi
if cargo tree -p rekuiper-core -p rekuiper-sql -i unsafe-libyaml 2>/dev/null; then
  echo "ERROR: unsafe-libyaml found in core crates"
  exit 1
fi
if cargo tree -p rekuiper-core -p rekuiper-sql -i rustls-webpki@0.102.8 2>/dev/null; then
  echo "ERROR: rustls-webpki 0.102.8 found in core crates"
  exit 1
fi
echo "Advisories and Isolation checks PASSED."

echo "=== [4/5] Running Clippy Warnings Check ==="
cargo clippy --workspace --all-targets --target-dir /home/paanday/target -- -D warnings
rm -rf /home/paanday/target

echo "=== [5/5] Running Cargo Test Suite (Crate-by-Crate with Disk Hygiene) ==="
for c in rekuiper-conf rekuiper-core rekuiper-sql rekuiper-connectors rekuiper-mcp rekuiper-cli; do
  echo ">>> Testing $c <<<"
  cargo test -p "$c" --target-dir /home/paanday/target
  rm -rf /home/paanday/target
done

echo ">>> Testing rekuiper-server lib & core integration suites <<<"
cargo test -p rekuiper-server --lib --target-dir /home/paanday/target
rm -rf /home/paanday/target

cargo test -p rekuiper-server --test test_otlp_and_ui --test test_declarative_mode --test test_rule_lifecycle --test test_dlq_poison_pill --test fvt_compat --target-dir /home/paanday/target -- test_jwt_authentication_vectors
rm -rf /home/paanday/target

echo ">>> Testing kuiperd <<<"
cargo test -p kuiperd --target-dir /home/paanday/target
rm -rf /home/paanday/target

echo ">>> Testing rule-engine & ruleengine <<<"
cargo test -p rule-engine -p ruleengine --target-dir /home/paanday/target
rm -rf /home/paanday/target

echo "=== ALL LOCAL CI CHECKS PASSED SUCCESSFULLY ==="
