# Installation and Deployment

rekuiper operates on diverse edge architectures. Supported platforms include single-board computers (Raspberry Pi, BeagleBone), industrial PCs, and edge servers.

---

## 1. Network Ports Reference

rekuiper uses these standard network ports:

| Port | Protocol | Service / Purpose | Default Accessibility |
| :--- | :--- | :--- | :--- |
| **`9081`** | HTTP / TCP | REST API, SSE event streaming, and eKuiper Manager UI backend | External (Host & LAN) |
| **`20498`** | TCP | NanoIPC / Edge IPC socket for high-speed local connector streaming | Internal / IPC |
| **`20499`** | TCP | Management RPC daemon for the `kuiper` CLI | Host / Internal |

---

## 2. Run with Docker

Docker provides a consistent deployment method for edge gateways.

### Quick Run

Start the container:

```shell
docker run -d \
  --name rekuiper \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  ankurkrp/rekuiper:1.0.0-rc3
```

### Production Deployment with Persistent Storage

For production environments, mount host directories for stream schemas, rule definitions, and state databases:

```shell
docker run -d \
  --name rekuiper \
  --restart unless-stopped \
  -p 9081:9081 \
  -p 20498:20498 \
  -p 20499:20499 \
  -v /var/lib/rekuiper/data:/kuiper/data \
  -v /var/lib/rekuiper/etc:/kuiper/etc \
  -v /var/log/rekuiper:/kuiper/log \
  -e KUIPER__BASIC__PROMETHEUS="true" \
  -e LOG_LEVEL="info" \
  ankurkrp/rekuiper:1.0.0-rc3
```

Verify service availability:

```shell
curl http://localhost:9081/ping
# Response: pong
```

---

## 3. Production Deployment with Docker Compose

You can deploy rekuiper with an MQTT broker (Eclipse Mosquitto) using Docker Compose.

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  rekuiper:
    image: ankurkrp/rekuiper:1.0.0-rc3
    container_name: rekuiper
    restart: unless-stopped
    ports:
      - "9081:9081"
      - "20498:20498"
      - "20499:20499"
    environment:
      KUIPER__BASIC__REST_PORT: "9081"
      KUIPER__BASIC__PROMETHEUS: "true"
      MQTT_SOURCE__DEFAULT__SERVER: "tcp://mosquitto:1883"
      RUST_LOG: "info"
    volumes:
      - rekuiper-data:/kuiper/data
      - rekuiper-etc:/kuiper/etc
    depends_on:
      - mosquitto
    networks:
      - edge-net

  mosquitto:
    image: eclipse-mosquitto:2
    container_name: mosquitto
    restart: unless-stopped
    ports:
      - "1883:1883"
    networks:
      - edge-net

volumes:
  rekuiper-data:
  rekuiper-etc:

networks:
  edge-net:
    driver: bridge
```

Start the containers:

```shell
docker compose up -d
```

Service endpoints:
- **rekuiper REST API**: `http://localhost:9081`
- **Prometheus Metrics**: `http://localhost:20499/metrics` (or `http://localhost:9081/metrics`)
- **MQTT Broker**: `localhost:1883`

---

## 4. Standalone Binary Installation

On bare-metal Linux gateways, OpenWrt devices, or systems without container runtimes, rekuiper can run as a single native binary.

### Download Prebuilt Binaries

Prebuilt binaries are available on [GitHub Releases](https://github.com/ankur-paan/rekuiper/releases):

```shell
# Example for Linux x86_64 (replace with desired release tag)
curl -LO https://github.com/ankur-paan/rekuiper/releases/download/v1.0.0-rc3/rekuiperd-x86_64-unknown-linux-musl.tar.gz
tar -xzf rekuiperd-x86_64-unknown-linux-musl.tar.gz
cd rekuiper

# Start the daemon
./bin/kuiperd
```

### Compile from Source

To compile rekuiper with specific CPU architecture optimizations:

**Prerequisites:** Rust 1.80 or later, and Cargo.

```shell
# Clone the repository
git clone https://github.com/ankur-paan/rekuiper.git
cd rekuiper

# Compile in release mode
cargo build --release

# The compiled binaries will be located at:
# target/release/kuiperd  (Server engine)
# target/release/kuiper   (CLI tool)
# target/release/rekuiper-mcp (MCP AI server)
```

Start the compiled daemon:

```shell
./target/release/kuiperd --etc etc --data data --log log
```

---

## 5. Configuration Reference

rekuiper reads configuration from `etc/kuiper.yaml`. The engine supports environment variable overrides that use a double-underscore (`__`) delimiter:

| Environment Variable | Config Equivalent | Default | Description |
| :--- | :--- | :--- | :--- |
| `KUIPER__BASIC__REST_PORT` | `basic.restPort` | `9081` | Port for the HTTP REST API |
| `KUIPER__BASIC__CLI_PORT` | `basic.cliPort` | `20499` | Port for the internal CLI management RPC |
| `KUIPER__BASIC__PROMETHEUS` | `basic.prometheus` | `false` | Enable Prometheus metrics at `/metrics` |
| `KUIPER__BASIC__CONSOLELOG` | `basic.consoleLog` | `true` | Log events directly to stdout / console |
| `LOG_LEVEL` | - | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `MQTT_SOURCE__DEFAULT__SERVERS` | `mqtt.default.servers` | `[tcp://127.0.0.1:1883]` | Default MQTT broker address list |

---

## 6. Systemd Service Setup (Linux)

To run rekuiper as a background system service on Linux gateways:

Create `/etc/systemd/system/rekuiper.service`:

```ini
[Unit]
Description=rekuiper Stream Processing Engine
After=network.target

[Service]
Type=simple
User=rekuiper
ExecStart=/opt/rekuiper/bin/kuiperd --etc /opt/rekuiper/etc --data /opt/rekuiper/data --log /opt/rekuiper/log
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```shell
sudo systemctl daemon-reload
sudo systemctl enable --now rekuiper
sudo systemctl status rekuiper
```

