# uBroker.Kafka

Confluent.Kafka implementation for uBroker with partitioned publish/consume and checkpoint support.

## Installation

```bash
dotnet add package uBroker.Kafka
```

## Overview

Built on `Confluent.Kafka` 2.x, this provider implements `IPartitionedPublisher` for partition-key routing and `ICheckpointableConsumer` for manual offset commit. Native batching via `linger.ms` provides high throughput without application-level batching.

## Quick Start

```csharp
using Microsoft.Extensions.DependencyInjection;
using uBroker;
using uBroker.Kafka;

// Register
services.AddUBrokerKafka(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.ConsumerGroup = "order-service";
    o.LingerMs = 5;
    o.BatchSize = 16384;
});

// Publish with partition key
var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("orders", new Order { Id = 1 },
    new PublishOptions { PartitionKey = "order-123" });

// Consume with checkpoint
var consumer = sp.GetRequiredService<ICheckpointableConsumer>();
consumer.Subscribe<Order>("orders", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.CheckpointAsync(); // commit offset
}, new ConsumeOptions { ConsumerGroup = "order-service" });
```

## Features

### Partitioned Publishing

Messages are routed to specific partitions via `PartitionKey`:

```csharp
await publisher.PublishAsync("orders", order,
    new PublishOptions { PartitionKey = $"order-{order.Id}" });
```

### Manual Offset Commit

Consumers explicitly checkpoint offsets for at-least-once delivery:

```csharp
consumer.Subscribe<Order>("orders", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.CheckpointAsync(); // commit offset to Kafka
});
```

### Consumer Groups

Each subscription requires a consumer group for offset tracking:

```csharp
consumer.Subscribe<Order>("orders", handler,
    new ConsumeOptions { ConsumerGroup = "my-service" });
```

### Raw Binary Support

Blittable structs with `[UBrokerRawBinary]` bypass JSON:

```csharp
[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct StockPrice
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}
```

## Options Reference

| Option | Type | Default | Description |
|---|---|---|---|
| `BootstrapServers` | `string` | — | Kafka broker addresses |
| `ConsumerGroup` | `string` | — | Default consumer group name |
| `LingerMs` | `int` | `5` | Batch linger time (ms) |
| `BatchSize` | `int` | `16384` | Max batch size (bytes) |
| `CompressionType` | `string` | `"none"` | Compression: none, gzip, snappy, lz4, zstd |
| `Acks` | `string` | `"all"` | Acknowledgment: all, leader, none |
| `EnableAutoCommit` | `bool` | `false` | Manual commit (recommended) |
| `AutoOffsetReset` | `string` | `"latest"` | Reset policy: latest, earliest |

## Observability

- **Meter**: `ubroker.publish.messages`, `ubroker.consume.messages`
- **Tracing**: `traceparent` header propagated via Kafka message headers
- **Serializer tag**: `"raw"` or `"json"` on all counters

## Requirements

- .NET 10.0 or later
- Confluent.Kafka 2.x
- Apache Kafka 2.0+ or Redpanda

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [Confluent.Kafka Documentation](https://github.com/confluentinc/confluent-kafka-dotnet)
- [Report Issues](https://github.com/theunal/uBroker/issues)
