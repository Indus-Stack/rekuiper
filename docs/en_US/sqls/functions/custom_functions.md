# Custom Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 16:47:58 UTC**.  
> **Scorecard**: **Plugin-Dependent Extensions**:  
> Custom functions (`echo`, `countPlusOne`, `accumulateWordCount`, image, geohash, and TensorFlow plugins) require compiling and registering external plugin dynamic libraries (`/plugins/functions`) or JavaScript UDF functions (`/udf/javascript`). Without the plugin loaded, the engine validates functions strictly and returns `function not found`.

rekuiper supports custom user-defined functions. For details on how to develop, compile, and install native function plugins, refer to [Native Function Extensions](../../extension/native/develop/function.md).

## echo Plugin

| Function | Example | Description |
| :--- | :--- | :--- |
| `echo` | `echo(avg)` | Outputs the input parameter value without modification. |

Example:

Assuming column `avg` has data type `int` with value `30`, the output is `[{"r1":30}]`:

```sql
SELECT echo(avg) as r1 FROM test;
```

## countPlusOne Plugin

| Function | Example | Description |
| :--- | :--- | :--- |
| `countPlusOne` | `countPlusOne(avg)` | Outputs the array length plus one. |

Example:

Assuming column `avg` has data type `[]int` with value `[1, 2, 3]`, the output is `[{"r1":4}]`:

```sql
SELECT countPlusOne(avg) as r1 FROM test;
```

## accumulateWordCount Plugin

| Function | Example | Description |
| :--- | :--- | :--- |
| `accumulateWordCount` | `accumulateWordCount(avg, sep)` | Counts the number of words separated by `sep`. |

Example:

Assuming `avg` is a string with value `"My name is Bob"` and `sep` is `" "`, the output is `[{"r1":4}]`:

```sql
SELECT accumulateWordCount(avg, sep) as r1 FROM test;
```

## Image Processing Plugin

Image processing functions support `png` and `jpeg` formats.

| Function | Example | Description |
| :--- | :--- | :--- |
| `resize` | `resize(avg, width, height, [isRaw])` | Scales an image to target dimensions. If width or height is 0, the function maintains the aspect ratio. The optional boolean `isRaw` specifies whether to return raw image bytes rather than encoded formats. |
| `thumbnail` | `thumbnail(avg, maxWidth, maxHeight)` | Scales down an image to maximum dimensions while preserving the aspect ratio. |

Example `resize(avg, width, height)`:

Input `avg` has data type `[]byte`:

```sql
SELECT resize(avg, width, height) as r1 FROM test;
```

Example `thumbnail(avg, maxWidth, maxHeight)`:

Input `avg` has data type `[]byte`:

```sql
SELECT thumbnail(avg, maxWidth, maxHeight) as r1 FROM test;
```

## Geohash Plugin

The Geohash plugin provides geospatial encoding and decoding functions:

| Function | Signature | Description |
| :--- | :--- | :--- |
| `geohashEncode` | `geohashEncode(la, lo float64)(string)` | Encodes latitude and longitude into a geohash string. |
| `geohashEncodeInt` | `geohashEncodeInt(la, lo float64)(uint64)` | Encodes latitude and longitude into an unsigned 64-bit integer geohash. |
| `geohashDecode` | `geohashDecode(hash string)(la, lo float64)` | Decodes a geohash string into latitude and longitude coordinates. |
| `geohashDecodeInt` | `geohashDecodeInt(hash uint64)(la, lo float64)` | Decodes an integer geohash into latitude and longitude coordinates. |
| `geohashBoundingBox` | `geohashBoundingBox(hash string)(string)` | Returns the boundary box coordinates encoded by a geohash string. |
| `geohashBoundingBoxInt` | `geohashBoundingBoxInt(hash uint64)(string)` | Returns the boundary box coordinates encoded by an integer geohash. |
| `geohashNeighbor` | `geohashNeighbor(hash string, direction string)(string)` | Returns the neighbor in the specified cardinal direction (`North`, `NorthEast`, `East`, `SouthEast`, `South`, `SouthWest`, `West`, `NorthWest`). |
| `geohashNeighborInt` | `geohashNeighborInt(hash uint64, direction string)(uint64)` | Returns the integer neighbor in the specified cardinal direction. |
| `geohashNeighbors` | `geohashNeighbors(hash string)([]string)` | Returns an array containing all eight neighboring geohash strings. |
| `geohashNeighborsInt` | `geohashNeighborsInt(hash uint64)([]uint64)` | Returns an array containing all eight neighboring integer geohashes. |

### Geohash Examples

Example `geohashEncode`:

- Input: `{"lo": 131.036192, "la": -25.345457}`
- Output: `{"geohashEncode": "qgmpvf18h86e"}`

```sql
SELECT geohashEncode(la, lo) FROM test
```

Example `geohashEncodeInt`:

- Input: `{"lo": 131.036192, "la": -25.345457}`
- Output: `{"geohashEncodeInt": 12963433097944239317}`

```sql
SELECT geohashEncodeInt(la, lo) FROM test
```

Example `geohashDecode`:

- Input: `{"hash": "qgmpvf18h86e"}`
- Output: `{"geohashDecode": {"Longitude": 131.036192, "Latitude": -25.345457099999997}}`

```sql
SELECT geohashDecode(hash) FROM test
```

Example `geohashDecodeInt`:

- Input: `{"hash": 12963433097944239317}`
- Output: `{"geohashDecodeInt": {"Longitude": 131.03618861, "Latitude": -25.345456300000002}}`

```sql
SELECT geohashDecodeInt(hash) FROM test
```

Example `geohashBoundingBox`:

- Input: `{"hash": "qgmpvf18h86e"}`
- Output: `{"geohashBoundingBox": {"MinLat": -25.345457140356302, "MaxLat": -25.34545697271824, "MinLng": 131.03619195520878, "MaxLng": 131.0361922904849}}`

```sql
SELECT geohashBoundingBox(hash) FROM test
```

Example `geohashBoundingBoxInt`:

- Input: `{"hash": 12963433097944239317}`
- Output: `{"geohashBoundingBoxInt": {"MinLat": -25.345456302165985, "MaxLat": -25.34545626025647, "MinLng": 131.0361886024475, "MaxLng": 131.03618868626654}}`

```sql
SELECT geohashBoundingBoxInt(hash) FROM test
```

Example `geohashNeighbor`:

- Input: `{"hash": "qgmpvf18h86e", "direction": "North"}`
- Output: `{"geohashNeighbor": "qgmpvf18h86s"}`

```sql
SELECT geohashNeighbor(hash, direction) FROM test
```

Example `geohashNeighborInt`:

- Input: `{"hash": 12963433097944239317, "direction": "North"}`
- Output: `{"geohashNeighborInt": 12963433097944240129}`

```sql
SELECT geohashNeighborInt(hash, direction) FROM test
```

Example `geohashNeighbors`:

- Input: `{"hash": "qgmpvf18h86e"}`
- Output: `{"geohashNeighbors": ["qgmpvf18h86s", "qgmpvf18h86u", "qgmpvf18h86g", "qgmpvf18h86f", "qgmpvf18h86d", "qgmpvf18h866", "qgmpvf18h867", "qgmpvf18h86k"]}`

```sql
SELECT geohashNeighbors(hash) FROM test
```

Example `geohashNeighborsInt`:

- Input: `{"hash": 12963433097944239317}`
- Output: `{"geohashNeighborsInt": [12963433097944240129, 12963433097944240131, 12963433097944240130, 12963433097944237399, 12963433097944237397, 12963433097944150015, 12963433097944152746, 12963433097944152747]}`

```sql
SELECT geohashNeighborsInt(hash) FROM test
```

## LabelImage Plugin

The `LabelImage` sample plugin runs TensorFlow Lite image classification models. This plugin is included in Docker images with the `-slim` suffix.

The function receives a `bytea` image payload and returns the predicted classification label.

Assuming input payload `self` contains binary data for `peacock.jpg`, the function outputs `"peacock"`:

```sql
SELECT labelImage(self) FROM tfdemo
```

## tfLite Plugin

The `tfLite` plugin executes TensorFlow Lite model inference. This plugin is included in Docker images with the `-slim` suffix.

To execute inference:
1. Upload the `.tflite` model using the [uploads](../../api/restapi/uploads.md) REST API.
2. Specify the model name without the `.tflite` extension in `model_name`.
3. Pass input data as a one-dimensional array in `input_data`.

```sql
SELECT tfLite(model_name, input_data) FROM tfdemo
```

## WebAssembly (Wasm) Plugin: wasm_run

```text
wasm_run(module_name, func_name, arg1, arg2, ...)
wasm(module_name, func_name, arg1, arg2, ...)
```

Executes an exported function from a compiled WebAssembly (`.wasm`) module registered in the embedded rekuiper Wasm runtime.

Modules are registered via the REST API (`POST /plugins/wasm`) or the MCP `register_wasm_plugin` tool. Exported functions can either be invoked directly as UDFs or dispatched dynamically via `wasm_run`.

### Parameters

- `module_name`: A string identifier matching the registered Wasm plugin module name (e.g. `'math'`, `'crypto'`, `'detector'`).
- `func_name`: A string identifying the exported function inside the Wasm module to execute.
- `arg1, arg2, ...`: Zero or more arguments passed directly to the Wasm function.

### Return Value

The calculated return value from the WebAssembly function execution. Returns `null` if the module or function does not exist, argument count/types mismatch, or execution encounters an internal trap.

### Examples

Execute an arithmetic addition function inside a Wasm module named `math`:

```sql
SELECT wasm_run('math', 'add', 10, 20) AS total FROM demo;
```

Result:

```json
{"total": 30}
```

Invoke an edge anomaly detection algorithm compiled from Rust or C to Wasm:

```sql
SELECT wasm('detector', 'evaluate_vibration', vibe, rpm) AS anomaly_score FROM telemetry;
```

