# Plugins Management

The rekuiper REST API manages native and portable plugins. You can create, list, inspect, update, register, and drop plugins.

> [!NOTE]
> Deleting a native plugin requires restarting the rekuiper server process. To update a plugin:
> 1. Delete the plugin.
> 2. Restart rekuiper.
> 3. Create the plugin with updated binaries or configuration.

## Create a Plugin

Use these endpoints to install and register a new plugin:

```http
POST http://localhost:9081/plugins/sources
POST http://localhost:9081/plugins/sinks
POST http://localhost:9081/plugins/functions
POST http://localhost:9081/plugins/portables
```

Request payload using a remote HTTP URL:

```json
{
  "name": "random",
  "file": "http://127.0.0.1/plugins/sources/random.zip"
}
```

Request payload using a local filesystem URI:

```json
{
  "name": "random",
  "file": "file:///var/plugins/sources/random.zip"
}
```

### Parameters

- `name`: The unique identifier of the plugin in lowerCamelCase (for example, `random` for `Random`).
- `file`: The URL or filesystem URI pointing to a `.zip` archive containing the compiled `.so` file and YAML metadata. For packaging details, refer to [Plugin Extension Overview](../../extension/overview.md).

### Plugin Package Structure

> [!NOTE]
> Legacy portable plugins are unsupported in rekuiper. For cross-language out-of-process services, refer to [External Services (gRPC/REST)](../../extension/external/external_func.md) and [WebAssembly Functions](../../extension/wasm/overview.md).

An example `random.zip` archive contains:
1. `Random@v1.0.0.so`
2. `random.yaml`
3. `install.sh`
4. Dependency files referenced by `install.sh` (for example, `mysdk.zip`, `myconfig.conf`)
5. `etc/`: Configuration files and runtime dependencies. The installer copies this folder to `<span v-pre>{{rekuiperPath}}/etc/{{pluginType}}</span>`.

Example `install.sh` dependency script:

```bash
#!/bin/sh
dir=/usr/local/mysdk
cur=$(dirname "$0")
echo "Base path $cur"
if [ -d "$dir" ]; then
    echo "SDK path $dir exists."
else
    echo "Creating SDK path $dir"
    mkdir -p $dir
    echo "Created SDK path $dir"
fi

apt install --no-upgrade unzip
if [ -d "$dir/lib" ]; then
    echo "SDK lib path $dir/lib exists."
else
    echo "Unzip SDK lib to path $dir"
    unzip $cur/mysdk.zip -d $dir
    echo "Unzipped SDK lib to path $dir"
fi

if [ -f "/etc/ld.so.conf.d/myconfig.conf" ]; then
    echo "/etc/ld.so.conf.d/myconfig.conf exists"
else
    echo "Copy conf file"
    cp $cur/myconfig.conf /etc/ld.so.conf.d/
    echo "Copied conf file"
fi
ldconfig
echo "Done"
```

## Show Plugins

Use these endpoints to list installed plugins for a specific plugin type:

```http
GET http://localhost:9081/plugins/sources
GET http://localhost:9081/plugins/sinks
GET http://localhost:9081/plugins/functions
GET http://localhost:9081/plugins/portables
```

Response sample:

```json
["plugin1", "plugin2"]
```

## Describe a Plugin

Use these endpoints to display metadata for an installed plugin:

```http
GET http://localhost:9081/plugins/sources/{name}
GET http://localhost:9081/plugins/sinks/{name}
GET http://localhost:9081/plugins/functions/{name}
GET http://localhost:9081/plugins/portables/{name}
```

Response sample:

```json
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## Drop a Plugin

Use these endpoints to delete an installed plugin:

```http
DELETE http://localhost:9081/plugins/sources/{name}
DELETE http://localhost:9081/plugins/sinks/{name}
DELETE http://localhost:9081/plugins/functions/{name}
DELETE http://localhost:9081/plugins/portables/{name}
```

For native plugins, you must restart the rekuiper server to complete deletion. For portable plugins, deletion takes effect immediately.

Append `?stop=1` to stop the rekuiper server process automatically upon deletion:

```http
DELETE http://localhost:9081/plugins/sources/{name}?stop=1
```

## Update a Plugin

Use these endpoints to update an installed plugin:

```http
PUT http://localhost:9081/plugins/sources/{name}
PUT http://localhost:9081/plugins/sinks/{name}
PUT http://localhost:9081/plugins/functions/{name}
PUT http://localhost:9081/plugins/portables/{name}
```

The request body matches the schema used for plugin creation.

## Portable Plugin Status

Use this endpoint to inspect the runtime process status of a portable plugin:

```http
GET http://localhost:9081/plugins/portables/{name}
```

Response sample:

```json
{
   "refCount": {
      "rulePort1": 2
   },
   "status": "running",
   "errMsg": "",
   "pid": 90
}
```

## Function Plugin Management

Function plugins can export multiple user-defined functions. Function names must be globally unique across all plugins.

### Show All User-Defined Functions

Use this endpoint to list all user-defined functions registered across all plugins:

```http
GET http://localhost:9081/plugins/udfs
```

Response sample:

```json
["func1", "func2"]
```

### Describe a User-Defined Function

Use this endpoint to identify the plugin that provides a specific function:

```http
GET http://localhost:9081/plugins/udfs/{name}
```

Response sample:

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### Register Functions for a Plugin

Use this endpoint to register exported function names for auto-loaded plugins:

```http
POST http://localhost:9081/plugins/functions/{plugin_name}/register
Content-Type: application/json

{
  "functions": ["func1", "func2"]
}
```

## WebAssembly (Wasm) Plugins

WebAssembly plugins provide sandboxed User-Defined Functions without server restarts.

### Register a WebAssembly Plugin

Use this endpoint to register a WebAssembly module:

```http
POST http://localhost:9081/plugins/wasm
Content-Type: application/json
```

Request with a local file path:

```json
{
  "name": "fibonacci",
  "file": "file:///plugins/wasm/fibonacci.wasm"
}
```

Request with base64 bytecode:

```json
{
  "name": "fibonacci",
  "bytecode": "AGFzbQEAAAA..."
}
```

Response sample (201 Created):

```json
{
  "name": "fibonacci",
  "functions": ["fib"]
}
```

### Show WebAssembly Plugins

Use this endpoint to list all installed WebAssembly modules:

```http
GET http://localhost:9081/plugins/wasm
```

Response sample:

```json
[
  {
    "name": "fibonacci",
    "functions": ["fib"]
  }
]
```

### Delete a WebAssembly Plugin

Use this endpoint to unload and remove a WebAssembly module:

```http
DELETE http://localhost:9081/plugins/wasm/{name}
```

The engine removes all exported functions immediately.
No server restart is necessary.

## Prebuilt Plugins (Legacy Compatibility)

> [!NOTE]
> Native Go `.so` plugins are not supported in rekuiper. Built-in connectors are compiled into the binary, and custom logic is loaded via WebAssembly or external services. The endpoints below exist for legacy eKuiper compatibility.

In legacy eKuiper, these endpoints query available prebuilt plugins configured in `pluginHosts` within `etc/kuiper.yaml`:

```http
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

Response sample:

```json
{
  "file": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/file_arm64.zip",
  "influx": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/influx_arm64.zip",
  "zmq": "http://127.0.0.1:63768/kuiper-plugins/0.9.1/sinks/alpine/zmq_arm64.zip"
}
```
