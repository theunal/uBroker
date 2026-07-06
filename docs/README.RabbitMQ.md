# uBroker.RabbitMQ

High-performance RabbitMQ implementation for uBroker with channel pooling and batch publishing.

## Installation

```bash
dotnet add package uBroker.RabbitMQ
```

## Overview

Built on `RabbitMQ.Client` 7.x, this provider delivers zero-allocation message publishing with an internal batch worker that coalesces messages for optimal throughput. Channel pooling eliminates the overhead of creating/destroying channels per publish.

## Quick Start

```csharp
using Microsoft.Extensions.DependencyInjection;
using uBroker;
using uBroker.RabbitMQ;

// Register
services.AddUBrokerRabbitMQ(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672";
    o.BatchMaxSize = 500;
    o.BatchMaxWindow = TimeSpan.FromMilliseconds(5);
});

// Publish
var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("orders", new Order { Id = 1 },
    new PublishOptions { RoutingKey = "order.created" });

// Consume
var consumer = sp.GetRequiredService<IUBrokerConsumer>();
consumer.Subscribe<Order>("orders.queue", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.AckAsync();
});
```

## Features

### Channel Pooling

Channels are pooled via `ObjectPool<IChannel>` to avoid per-publish allocation overhead. Pool size is configurable:

```csharp
o.ChannelPoolSize = 16; // default: Environment.ProcessorCount
```

### Batch Publishing

The `BatchPublishWorker` (BackgroundService) coalesces publish requests using `System.Threading.Channels`:

- **BatchMaxSize**: 500 messages (default) — flush when batch is full
- **BatchMaxWindow**: 5ms (default) — flush when time window expires
- Flush triggers: whichever hits first

```csharp
o.BatchMaxSize = 500;    // max messages per batch
o.BatchMaxWindow = TimeSpan.FromMilliseconds(5); // max wait time
```

### QoS Configuration

```csharp
o.PrefetchCount = 50; // max unacknowledged messages delivered to consumer
```

### Dead-Letter Exchange

```csharp
consumer.Subscribe<Order>("orders.queue", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.AckAsync();
}, new ConsumeOptions
{
    DeadLetterExchange = "orders.dlx"
});
```

### Raw Binary Support

Blittable structs with `[UBrokerRawBinary]` attribute are automatically serialized via direct memory blit, bypassing JSON entirely:

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
| `ConnectionString` | `string` | — | AMQP connection string |
| `BatchMaxSize` | `int` | `500` | Max messages per batch flush |
| `BatchMaxWindow` | `TimeSpan` | `5ms` | Max wait time before flush |
| `ChannelPoolSize` | `int` | `ProcessorCount` | Number of channels in pool |
| `PrefetchCount` | `ushort` | `50` | QoS prefetch count |

## Observability

- **Meter**: `ubroker.publish.messages`, `ubroker.consume.messages`, `ubroker.publish.batch.size`
- **Tracing**: `traceparent` header propagated via AMQP headers
- **Diagnostics**: channel pool rent/return counters, batch flush reason (timeout vs size)

## Requirements

- .NET 10.0 or later
- RabbitMQ.Client 7.x
- RabbitMQ 3.8+ server

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [RabbitMQ.Client 7.x Documentation](https://github.com/rabbitmq/rabbitmq-dotnet-client)
- [Report Issues](https://github.com/theunal/uBroker/issues)
