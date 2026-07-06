# uBroker

High-performance, zero-allocation .NET 10 multi-broker messaging library.

## Architecture

```
uBroker.Core              Broker-agnostic interfaces & options
  ├─ IUBrokerPublisher    Base publish interface
  ├─ IUBrokerConsumer     Base consume interface
  ├─ IPartitionedPublisher  Capability: partitioned publish (Kafka, Event Hubs)
  └─ ICheckpointableConsumer  Capability: checkpoint-based consume (Kafka, Event Hubs)

uBroker.RabbitMQ          RabbitMQ.Client 7.x — channel pooling, batching
uBroker.Kafka             Confluent.Kafka — partitioned publish, offset commit
uBroker.Azure             Azure Service Bus + Event Hubs (blob checkpoint)
uBroker.Aws               AWS SQS (queue) + SNS (pub/sub, publish-only)

uBroker.DependencyInjection   services.AddUBrokerXxx() per provider
uBroker.Diagnostics            Meter + ActivitySource (OpenTelemetry)
```

### Why Core + Capability Interfaces?

RabbitMQ/SQS use **queue** semantics (ack/nack per message).
Kafka/Event Hubs use **partitioned log** semantics (offset checkpoint per batch).
SNS uses **pub/sub fan-out** (no consumer — publishes to SQS topics).

Forcing all into one interface would create `NotSupportedException` hell.
Instead, the base `IUBrokerPublisher`/`IUBrokerConsumer` covers the common case,
and `IPartitionedPublisher`/`ICheckpointableConsumer` extend for log-based brokers.

## Quick Start

```csharp
// RabbitMQ
services.AddUBrokerRabbitMQ(o => o.ConnectionString = "amqp://guest:guest@localhost:5672");

// Kafka
services.AddUBrokerKafka(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.ConsumerGroup = "order-service";
});

// Azure Service Bus
services.AddUBrokerAzureServiceBus(o => o.ConnectionString = "...");

// Azure Event Hubs
services.AddUBrokerAzureEventHubs(o =>
{
    o.ConnectionString = "...";
    o.CheckpointStorageConnectionString = "...";
});

// AWS SQS
services.AddUBrokerAwsSqs(o => o.Region = "eu-central-1");

// AWS SNS (publish-only)
services.AddUBrokerAwsSns(o => o.Region = "eu-central-1");
```

### Publishing

```csharp
var publisher = sp.GetRequiredService<IUBrokerPublisher>();

// Simple publish (works with all providers)
await publisher.PublishAsync("orders", new Order { Id = 1 });

// With provider-specific options
await publisher.PublishAsync("orders", order, new PublishOptions
{
    RoutingKey = "order.created",           // RabbitMQ
    PartitionKey = "order-123",             // Kafka, Event Hubs
    SessionId = "session-1",                // Azure Service Bus
    MessageGroupId = "group-1",             // AWS SQS FIFO
});
```

### Consuming

```csharp
var consumer = sp.GetRequiredService<IUBrokerConsumer>();

consumer.Subscribe<Order>("orders.queue", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.AckAsync();  // Queue-based: ack
});

// Kafka/Event Hubs: checkpoint-based
var partitionedConsumer = sp.GetRequiredService<ICheckpointableConsumer>();
partitionedConsumer.Subscribe<Order>("orders-topic", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.CheckpointAsync();  // Log-based: checkpoint offset
});
```

### Keyed Services (Multiple Providers)

```csharp
// When using multiple providers simultaneously
services.AddKeyedSingleton<IUBrokerPublisher>("rabbitmq", sp => ...);
services.AddKeyedSingleton<IUBrokerPublisher>("kafka", sp => ...);

// Inject with [FromKeyedServices("kafka")]
```

## Provider Details

| Provider | Package | Publish | Consume | Batching | Partitioning |
|---|---|---|---|---|---|
| RabbitMQ | `uBroker.RabbitMQ` | exchange + routing key | queue ack/nack | Channel<T> batch worker | No |
| Kafka | `uBroker.Kafka` | topic + partition key | consumer group + offset commit | Native (linger.ms) | Yes |
| Azure Service Bus | `uBroker.Azure` | queue/topic | processor + session | Native batch | No |
| Azure Event Hubs | `uBroker.Azure` | event hub + partition key | EventProcessorClient + blob checkpoint | Native EventDataBatch | Yes |
| AWS SQS | `uBroker.Aws` | queue URL | long poll + visibility timeout | SendMessageBatch (max 10) | FIFO support |
| AWS SNS | `uBroker.Aws` | topic ARN (publish only) | N/A — SNS→SQS fan-out | N/A | FIFO support |

## Observability

All providers share:
- **Meter** (`uBroker`):
  - `ubroker.publish.messages` — total messages published (tag: `serializer`)
  - `ubroker.consume.messages` — total messages consumed (tag: `serializer`)
  - `ubroker.publish.batch.size` — histogram of batch sizes at flush
  - `ubroker.publish.latency` — end-to-end publish latency (ms)
  - `ubroker.publish.errors` — failed deliveries
  - `ubroker.channel.pool.rent` / `ubroker.channel.pool.return` — channel pool ops
  - `ubroker.channel.active` — currently rented channels
- **Tracing** (`ActivitySource`): `traceparent` header propagated via each broker's native header mechanism
- **Health Checks**: connection/client status per provider

## High-Performance Raw Binary Path

For high-throughput scenarios with fixed-size blittable structs (stock prices, IoT telemetry, metrics), uBroker provides a zero-allocation raw binary serialization path that bypasses JSON entirely.

### When to Use

- Structs with only primitive/fixed-size fields (`int`, `decimal`, `long`, `enum`, etc.)
- Both producer and consumer are .NET/uBroker (cross-language not supported)
- Little-endian platform (all mainstream .NET deployments)
- Throughput-critical paths where JSON serialization is a bottleneck

### How to Opt-In

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

That's it. Publishers and consumers automatically detect the attribute and use the raw binary path. No API changes needed.

### Constraints

- **Struct layout required**: `[StructLayout(LayoutKind.Sequential)]` or `LayoutKind.Explicit` must be present. Without it, `InvalidOperationException` is thrown at first access.
- **No reference types**: Structs containing `string`, `class`, `object`, or pointer fields are silently excluded from the raw path (falls back to JSON).
- **No schema versioning**: Changing the struct layout breaks wire compatibility. There is no version negotiation.
- **.NET-to-.NET only**: Non-.NET consumers cannot read raw binary payloads.
- **No cross-endian**: Raw binary assumes little-endian byte order.

### What Happens Under the Hood

1. Publisher checks `RawBinaryTypeInfo<T>.IsEligible` (cached per type, zero reflection per call)
2. If eligible, serializes via `Unsafe.As` + `Unsafe.CopyBlock` (zero allocation, no JSON)
3. Sets `content-type: application/x-ubroker-raw` header on the broker message
4. Consumer reads the header and routes to `UnmanagedBlitSerializer.Read<T>()`

### Running Benchmarks

```bash
dotnet run --project benchmarks/uBroker.Benchmarks -c Release -- --filter *RawBinary*
```

## Local Development

```bash
# Start all broker emulators (RabbitMQ, Kafka KRaft, Azure SB, Azurite, LocalStack)
docker compose up -d

# Tear down including volumes
docker compose down -v
```

| Service | Port | Purpose |
|---|---|---|
| RabbitMQ | 5673 (AMQP), 15672 (UI) | Management UI at localhost:15672 |
| Kafka | 9092 | KRaft mode, no ZooKeeper |
| Azure SB Emulator | 5672 | Pre-configured queues/topics |
| Azurite | 10000-10002 | Blob/Queue/Table storage |
| LocalStack | 4566 | SQS + SNS emulation |

## Requirements

- .NET 10 SDK
- Docker (for local development and integration tests)
- RabbitMQ 4.3+ (for RabbitMQ provider)
- Kafka/Redpanda (for Kafka provider)
- Azure subscription (for Service Bus / Event Hubs providers)
- AWS account (for SQS / SNS providers)
