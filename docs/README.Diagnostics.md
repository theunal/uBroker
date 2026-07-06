# uBroker.Diagnostics

OpenTelemetry observability layer for uBroker. Provides metrics via `System.Diagnostics.Metrics` and distributed tracing via `ActivitySource`.

## Installation

```bash
dotnet add package uBroker.Diagnostics
```

## Overview

`uBroker.Diagnostics` is automatically registered when using any uBroker provider via dependency injection. It instruments all publish/consume operations with metrics and traces for monitoring, alerting, and debugging.

## Metrics

All metrics use the `uBroker` meter name.

### Counters

| Metric | Unit | Description | Tags |
|---|---|---|---|
| `ubroker.publish.messages` | `{messages}` | Total messages published | `serializer`: `"raw"` or `"json"` |
| `ubroker.consume.messages` | `{messages}` | Total messages consumed | `serializer`: `"raw"` or `"json"` |
| `ubroker.publish.batch.flush.timeout` | `{flushes}` | Batch flushes triggered by time window | — |
| `ubroker.publish.batch.flush.size` | `{flushes}` | Batch flushes triggered by batch size limit | — |
| `ubroker.publish.errors` | `{errors}` | Failed publish operations | — |
| `ubroker.channel.pool.rent` | `{operations}` | Channel pool rent operations | — |
| `ubroker.channel.pool.return` | `{operations}` | Channel pool return operations | — |

### Histograms

| Metric | Unit | Description |
|---|---|---|
| `ubroker.publish.batch.size` | `messages` | Distribution of batch sizes at flush time |
| `ubroker.publish.latency` | `ms` | End-to-end publish latency |

### Up-Down Counters

| Metric | Unit | Description |
|---|---|---|
| `ubroker.channel.active` | `{channels}` | Currently rented (active) channels |

## Tracing

Distributed tracing uses `System.Diagnostics.ActivitySource` with the name `uBroker`.

### Publish Traces

Each publish creates an `Activity` with kind `Producer`:

```csharp
// Activity name: "uBroker.Publish {exchange} {routingKey}"
// Tags:
//   messaging.system = "rabbitmq"
//   messaging.destination.name = {exchange}
//   messaging.rabbitmq.routing_key = {routingKey}
```

### Consume Traces

Each consume creates an `Activity` with kind `Consumer`:

```csharp
// Activity name: "uBroker.Consume {queue}"
// Tags:
//   messaging.system = "rabbitmq"
//   messaging.destination.name = {queue}
```

### Trace Propagation

The `traceparent` header is propagated through each broker's native header mechanism:
- RabbitMQ: `BasicProperties.Headers`
- Kafka: `MessageHeader`
- Service Bus: `ApplicationProperties`
- Event Hubs: `EventData.Properties`
- SQS/SNS: `MessageAttributes`

## Integration with OpenTelemetry

```csharp
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddOpenTelemetry()
            .WithMetrics(metrics => metrics
                .AddMeter("uBroker")
                .AddOtlpExporter())
            .WithTracing(tracing => tracing
                .AddSource("uBroker")
                .AddOtlpExporter());

        services.AddUBrokerRabbitMQ(o => o.ConnectionString = "...");
    })
    .Build();
```

## Integration with Prometheus

```csharp
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("uBroker")
        .AddPrometheusExporter());
```

## Requirements

- .NET 10.0 or later
- Depends only on `uBroker.Core` (no broker-specific dependencies)

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [OpenTelemetry .NET Documentation](https://github.com/open-telemetry/opentelemetry-dotnet)
- [Report Issues](https://github.com/theunal/uBroker/issues)
