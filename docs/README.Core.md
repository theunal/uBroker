# uBroker.Core

High-performance, zero-allocation broker-agnostic messaging abstractions for .NET 10.

## Installation

```bash
dotnet add package uBroker.Core
```

## Overview

`uBroker.Core` defines the core interfaces and types that all broker providers implement. It provides a unified API for publishing and consuming messages across RabbitMQ, Kafka, Azure Service Bus, Azure Event Hubs, AWS SQS, and AWS SNS — without vendor lock-in.

## Interfaces

### IUBrokerPublisher

Publish messages to any supported broker. The `destination` parameter is mapped to the broker's native concept:

| Broker | Destination maps to |
|---|---|
| RabbitMQ | Exchange name |
| Kafka | Topic |
| Azure Service Bus | Queue or topic name |
| Azure Event Hubs | Event hub name |
| AWS SQS | Queue URL or name |
| AWS SNS | Topic ARN or name |

```csharp
public interface IUBrokerPublisher
{
    ValueTask PublishAsync<T>(
        string destination,
        T message,
        PublishOptions? options = null,
        CancellationToken ct = default);
}
```

### IUBrokerConsumer

Consume messages from queue-based brokers (RabbitMQ, SQS, Service Bus). Returns `IDisposable` for RAII unsubscribe.

```csharp
public interface IUBrokerConsumer
{
    IDisposable Subscribe<T>(
        string destination,
        Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null);
}
```

### IPartitionedPublisher

Extended publish interface for log-based brokers that support partition routing (Kafka, Event Hubs).

```csharp
public interface IPartitionedPublisher
{
    ValueTask PublishAsync<T>(
        string destination,
        T message,
        PublishOptions? options = null,
        CancellationToken ct = default);
}
```

### ICheckpointableConsumer

Extended consume interface for log-based brokers with offset checkpointing (Kafka, Event Hubs).

```csharp
public interface ICheckpointableConsumer
{
    IDisposable Subscribe<T>(
        string destination,
        Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null);
}
```

## PublishOptions

Broker-agnostic per-publish options. Each provider reads only the fields relevant to its protocol:

```csharp
public sealed class PublishOptions
{
    public string? RoutingKey { get; set; }        // RabbitMQ
    public string? PartitionKey { get; set; }       // Kafka, Event Hubs
    public string? SessionId { get; set; }          // Azure Service Bus
    public string? MessageGroupId { get; set; }     // AWS SQS FIFO, SNS FIFO
    public string? DeduplicationId { get; set; }    // AWS SQS FIFO
    public int? TimeToLiveMs { get; set; }          // RabbitMQ (ms), SQS (s)
    public byte? DeliveryMode { get; set; }         // RabbitMQ (1=transient, 2=persistent)
    public IDictionary<string, object>? Headers { get; set; }
}
```

## ConsumeOptions

Broker-agnostic per-subscription options:

```csharp
public sealed class ConsumeOptions
{
    public string? ConsumerGroup { get; set; }       // Kafka, Event Hubs (required)
    public ushort? PrefetchCount { get; set; }       // RabbitMQ, Service Bus
    public int MaxConcurrentCalls { get; set; }      // All providers (default: ProcessorCount)
    public string? DeadLetterExchange { get; set; }  // RabbitMQ
    public bool AutoAck { get; set; }                // RabbitMQ, Service Bus
    public int? BatchSize { get; set; }              // Event Hubs
    public int? VisibilityTimeoutSeconds { get; set; } // SQS
    public int? WaitTimeSeconds { get; set; }        // SQS (long-poll)
}
```

## Raw Binary Fast-Path

For high-throughput scenarios with fixed-size blittable structs, uBroker provides a zero-allocation serialization path that bypasses JSON entirely:

```csharp
using System.Runtime.InteropServices;
using uBroker;

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct StockPriceEvent
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}
```

### How it works

1. Publisher checks `RawBinaryTypeInfo<T>.IsEligible` (cached per type, zero reflection per call)
2. If eligible, serializes via `Unsafe.As` + `Unsafe.CopyBlock` (zero allocation)
3. Sets `content-type: application/x-ubroker-raw` header on the broker message
4. Consumer reads the header and routes to `UnmanagedBlitSerializer.Read<T>()`

### Performance

Benchmark results (AMD Ryzen 7 4800H, .NET 10.0):

| Method | Mean | Allocated |
|---|---|---|
| JsonSerialize | 55.05 ns | 184 B |
| RawSerialize | 0.25 ns | 0 B |
| JsonDeserialize | 208.53 ns | 232 B |
| RawDeserialize | 1.76 ns | 0 B |

- **223x faster** serialization
- **118x faster** deserialization
- **Zero heap allocation** on the raw binary path

### Constraints

- Structs must have `[StructLayout(LayoutKind.Sequential)]` or `LayoutKind.Explicit`
- No reference types (string, class, object) — falls back to JSON silently
- No schema versioning — changing struct layout breaks wire compatibility
- .NET-to-.NET only (not cross-language)
- Little-endian only (all mainstream .NET deployments)

## Wire Format

| Content-Type | Value |
|---|---|
| JSON | `application/json` |
| Raw Binary | `application/x-ubroker-raw` |

## Requirements

- .NET 10.0 SDK or later

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [Report Issues](https://github.com/theunal/uBroker/issues)
- [License (MIT)](https://github.com/theunal/uBroker/blob/main/LICENSE)
