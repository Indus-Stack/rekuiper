#!/bin/bash
set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

echo "[*] Syncing updated source files to WSL /home/paanday/rekuiper..."
cp /mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/crates/rekuiper-server/src/ui.html /home/paanday/rekuiper/crates/rekuiper-server/src/ui.html
cp /mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/crates/rekuiper-server/src/routes.rs /home/paanday/rekuiper/crates/rekuiper-server/src/routes.rs
cp /mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/crates/rekuiper-server/src/fleet.rs /home/paanday/rekuiper/crates/rekuiper-server/src/fleet.rs
cp /mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/scripts/manage_fleet.py /home/paanday/fleet/manage_fleet.py
chmod +x /home/paanday/fleet/manage_fleet.py

echo "[*] Staging ui.html for live dynamic serving..."
mkdir -p /home/paanday/fleet/control/etc
cp /home/paanday/rekuiper/crates/rekuiper-server/src/ui.html /home/paanday/fleet/ui.html
cp /home/paanday/rekuiper/crates/rekuiper-server/src/ui.html /home/paanday/fleet/control/etc/ui.html

echo "[*] Compiling kuiperd binary in WSL..."
cd /home/paanday/rekuiper
cargo build -p kuiperd --target-dir /home/paanday/target

echo "[*] Installing binary to /home/paanday/fleet/bin/kuiperd..."
cp /home/paanday/target/debug/kuiperd /home/paanday/fleet/bin/kuiperd
chmod +x /home/paanday/fleet/bin/kuiperd

echo "[*] Binary ready:"
ls -lh /home/paanday/fleet/bin/kuiperd

echo "[*] Restarting fleet..."
cd /home/paanday/fleet
python3 -u manage_fleet.py
