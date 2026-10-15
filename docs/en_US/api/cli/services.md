# Services Management

The rekuiper service CLI manages external services and service functions. You can register, list, describe, and drop services.

## Register a Service

Use this command to register an external service using a JSON specification:

```shell
create service $service_name $service_json
```

Before you run the command, store the service package archive at a location accessible to rekuiper.

Example command:

```shell
# bin/kuiper create service sample '{"name": "sample","file": "file:///tmp/sample.zip"}'
```

This command creates a service named `sample` using the package file at `file:///tmp/sample.zip`.

## Show Services and Service Functions

Use these commands to list all registered services and their associated functions:

List services:

```shell
# bin/kuiper show services
```

List service functions:

```shell
# bin/kuiper show service_funcs
```

## Describe a Service

Use this command to display configuration details and interfaces for a service:

```shell
describe service $service_name
```

Example command and output:

```shell
# bin/kuiper describe service sample
{
  "About": {
    "author": {
      "name": "Author Name",
      "email": "developer@example.org",
      "company": "Example Corp",
      "website": "https://example.org"
    },
    "helpUrl": {
      "en_US": "https://github.com/ankur-paan/rekuiper"
    },
    "description": {
      "en_US": "Sample external services for test only",
      "zh_CN": "示例外部函数配置，仅供测试"
    }
  },
  "Interfaces": {
    "trueno": {
      "Desc": null,
      "Addr": "tcp://localhost:50051",
      "Protocol": "grpc",
      "Schema": {
        "SchemaType": "protobuf",
        "SchemaFile": "sample.proto"
      },
      "Functions": [
        "label"
      ],
      "Options": null
    }
  }
}
```

## Describe a Service Function

Use this command to display details about an individual service function:

```shell
describe service_func $func_name
```

Example command and output:

```shell
# bin/kuiper describe service_func label
{
  "ServiceName": "serviceName",
  "InterfaceName": "interfaceName",
  "Addr": "http://192.168.2.102:9090",
  "MethodName": "funcName",
  "FuncName": "label"
}
```

## Drop a Service

Use this command to delete a service:

```shell
drop service $service_name
```

Example command:

```shell
# bin/kuiper drop service sample
```
