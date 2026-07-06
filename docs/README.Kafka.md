# uBroker.Kafka

Confluent.Kafka implementation for uBroker. Partitioned publish/consume with checkpoint support.

## Installation

```bash
dotnet add package uBroker.Kafka
```

## Quick Start

```csharp
services.AddUBrokerKafka(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.ConsumerGroup = "my-service";
});

var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("topic", message, new PublishOptions { PartitionKey = "key-1" });

var consumer = sp.GetRequiredService<ICheckpointableConsumer>();
consumer.Subscribe<Order>("topic", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.CheckpointAsync();
}, new ConsumeOptions { ConsumerGroup = "my-group" });
```

## Features

- `IPartitionedPublisher` — partition key routing
- `ICheckpointableConsumer` — manual offset commit
- Native batching via `linger.ms`
- Raw binary support via `[UBrokerRawBinary]`

## Links

- [GitHub](https://github.com/theunal/uBroker)
