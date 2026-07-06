# uBroker.Diagnostics

OpenTelemetry observability layer for uBroker. Meter + ActivitySource.

## Installation

```bash
dotnet add package uBroker.Diagnostics
```

## Metrics

| Metric | Type | Description |
|---|---|---|
| `ubroker.publish.messages` | Counter | Total messages published (tag: `serializer`) |
| `ubroker.consume.messages` | Counter | Total messages consumed (tag: `serializer`) |
| `ubroker.publish.batch.size` | Histogram | Batch sizes at flush |
| `ubroker.publish.latency` | Histogram | End-to-end publish latency (ms) |
| `ubroker.publish.errors` | Counter | Failed deliveries |
| `ubroker.channel.pool.rent` | Counter | Channel pool rent operations |
| `ubroker.channel.active` | UpDownCounter | Currently rented channels |

## Tracing

`traceparent` header propagated via each broker's native header mechanism.

## Links

- [GitHub](https://github.com/theunal/uBroker)
