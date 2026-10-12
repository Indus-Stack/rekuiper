# Video Source Connector

The **Video source connector** ingests live RTSP camera feeds, HTTP video streams, and video files into `rekuiper` streaming pipelines. It extracts discrete image frames at a configurable cadence and emits them as stream events.

> [!NOTE]
> **Architecture**
> The video connector is a built-in component. It does not require a dynamic shared library.
> It starts an FFmpeg process with asynchronous pipes.
> It divides the video stream into discrete JPEG frames by reading SOI (`0xFF 0xD8`) and EOI (`0xFF 0xD9`) markers.

---

## Prerequisites

The host system or container must have **FFmpeg** installed and accessible in the system `PATH` (or specified via `ffmpegPath`):

```bash
# Ubuntu / Debian
apt-get install -y ffmpeg

# Alpine Linux
apk add ffmpeg

# Verify installation
ffmpeg -version
```

---

## Configuration Properties

Configurations can be supplied via configuration files (`etc/sources/video.yaml`), the REST API (`/config/sources/video`), or specified inline in the stream DDL `WITH` clause:

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `url` | string | `""` | Video stream address (e.g., `rtsp://camera1.local:8554/live`, `http://...`, or local file path). |
| `interval` | integer / string | `1000` | Frame capture interval. Accepts milliseconds (e.g. `1000`) or duration strings (e.g. `"500ms"`, `"1s"`, `"2m"`). Default is `1000` (1 frame per second). |
| `codec` | string | `"mjpeg"` | Frame extraction codec for `image2pipe` streaming. Default is `"mjpeg"`. |
| `debugResp` | boolean | `false` | When `true`, logs FFmpeg process `stderr` output to the engine logs for diagnostics. |
| `inputArgs` | object | `{}` | Key-value mapping of custom input parameters passed to FFmpeg prior to `-i` (e.g., `{"rtsp_transport": "tcp"}`). |
| `ffmpegPath` | string | `"ffmpeg"` | Path or executable binary name of the FFmpeg binary on the host system. |

---

## Configuration File Example (`etc/sources/video.yaml`)

```yaml
default:
  url: rtsp://localhost:8554/stream
  interval: 1000
  codec: mjpeg
  debugResp: false
  inputArgs:
    rtsp_transport: tcp

high_frequency:
  url: rtsp://camera-fast.local:8554/feed
  interval: 200ms
  codec: mjpeg
  debugResp: true
```

---

## Stream DDL Definition

Define a video stream using `TYPE = "video"`:

```sql
CREATE STREAM camera_stream () WITH (
  TYPE = "video",
  URL = "rtsp://camera1.local:8554/live",
  INTERVAL = "1000",
  FORMAT = "json"
);
```

Or reference a pre-configured profile using `CONF_KEY`:

```sql
CREATE STREAM factory_floor_stream () WITH (
  TYPE = "video",
  CONF_KEY = "high_frequency",
  FORMAT = "json"
);
```

---

## Emitted Event Schema

Each captured frame produces a stream record containing:

| Field | Type | Description |
| :--- | :--- | :--- |
| `image` | string | Base64-encoded JPEG image binary bytes. |
| `url` | string | Origin video stream URL. |
| `size` | integer | Size of the JPEG image in bytes. |
| `format` | string | Encoding format (`"mjpeg"`). |
| `ts` | bigint | Unix millisecond timestamp when the frame was extracted. |

---

## Pipeline Examples

### 1. Ingest Video Frames and Store to Disk with Retention

Pair the video source stream with the native [Image Sink](../../sinks/plugin/image.md) to store snapshots and automatically prune files older than 48 hours:

```json
{
  "id": "archive_camera_snapshots",
  "sql": "SELECT image, url, ts FROM camera_stream",
  "actions": [
    {
      "image": {
        "path": "/data/camera_snapshots/camera1",
        "format": "jpg",
        "fileName": "frame_{{.ts}}.jpg",
        "maxAge": 48,
        "maxCount": 5000
      }
    }
  ]
}
```

### 2. Forward Extracted Frames over MQTT

```sql
CREATE RULE forward_frames_rule ()
AS
SELECT image, size, ts FROM camera_stream
SINK mqtt WITH (
  server = "tcp://127.0.0.1:1883",
  topic = "vision/frames/camera1"
);
```
