# uBroker.Core

Broker-agnostic messaging abstractions for .NET. Zero-allocation, high-performance.

## Installation

```bash
dotnet add package uBroker.Core
```

## Interfaces

- `IUBrokerPublisher` — publish messages to any broker
- `IUBrokerConsumer` — consume messages from any broker
- `IPartitionedPublisher` — partitioned publish (Kafka, Event Hubs)
- `ICheckpointableConsumer` — checkpoint-based consume (Kafka, Event Hubs)
- `IMessageSerializer` — zero-allocation JSON serialization via `IBufferWriter<byte>`

## Raw Binary Fast-Path

Blittable structs bypass JSON entirely via `[UBrokerRawBinary]` attribute:

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

**223x faster** serialization, **zero heap allocation**.

## Links

- [GitHub](https://github.com/theunal/uBroker)
- [License](https://github.com/theunal/uBroker/blob/main/LICENSE)
