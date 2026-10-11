# Image Sink

The `image` sink saves binary camera frame payloads (JPEG or PNG) directly to local storage directories with automated age-based (`maxAge`) and count-based (`maxCount`) retention pruning.

In `rekuiper`, the Image sink is built into the binary in pure Rust using asynchronous file I/O (`tokio::fs`) with zero C dynamic dependencies.

## Configuration Properties

Configure the `image` sink action using the following properties:

| Property Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | String | **Yes** | - | Target directory path for saved images (e.g. `/tmp/images` or <code v-pre>/tmp/images/{{.deviceId}}</code>). |
| `format` | String | No | `jpeg` | Image format: `jpeg`, `jpg`, or `png`. |
| `imageField` | String | No | auto | Name of the field containing binary bytes or a base64 encoded string. If omitted, searches `image`, `image_bytes`, `data`, and `payload`. |
| `fileName` | String | No | auto | Optional file name template (e.g. <code v-pre>frame_{{.deviceId}}_{{.ts}}.png</code>). If omitted, generates `uuid.format`. |
| `maxAge` | Integer | No | `72` | Maximum retention time in hours. Files older than this threshold are pruned. |
| `maxCount` | Integer | No | `1000` | Maximum number of stored image files in the target directory. When exceeded, the oldest files are removed. |

## Examples

### Storing Frame with Dynamic Path and Custom Name

Writes frames from camera streams into device-specific directories with custom file names:

```json
{
  "id": "rule_camera_save",
  "sql": "SELECT deviceId, image, ts FROM cameraStream",
  "actions": [
    {
      "image": {
        "path": "/data/captures/{{.deviceId}}",
        "format": "png",
        "imageField": "image",
        "fileName": "{{.deviceId}}_{{.ts}}.png",
        "maxAge": 24,
        "maxCount": 500
      }
    }
  ]
}
```
