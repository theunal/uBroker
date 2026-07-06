# uBroker.Azure

Azure Service Bus and Event Hubs implementation for uBroker.

## Installation

```bash
dotnet add package uBroker.Azure
```

## Overview

This package provides two Azure messaging providers:

- **Azure Service Bus**: Queue/topic publish with processor-based consume and session support
- **Azure Event Hubs**: Partitioned publish with `EventProcessorClient` and blob-based checkpointing

## Quick Start

### Service Bus

```csharp
using Microsoft.Extensions.DependencyInjection;
using uBroker;
using uBroker.Azure.ServiceBus;

// Register
services.AddUBrokerAzureServiceBus(o =>
{
    o.ConnectionString = "Endpoint=sb://your-ns.servicebus.windows.net/;...";
});

// Publish
var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("orders", new Order { Id = 1 });

// Consume
var consumer = sp.GetRequiredService<IUBrokerConsumer>();
consumer.Subscribe<Order>("orders", async (order, ctx) =>
{
    await ProcessOrder(order);
    await ctx.AckAsync();
});
```

### Event Hubs

```csharp
using uBroker.Azure.EventHubs;

// Register
services.AddUBrokerAzureEventHubs(o =>
{
    o.ConnectionString = "Endpoint=sb://your-ns.servicebus.windows.net/;...";
    o.CheckpointStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=...";
});

// Publish with partition key
var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("events", new Event { Id = 1 },
    new PublishOptions { PartitionKey = "event-123" });

// Consume with checkpoint
var consumer = sp.GetRequiredService<ICheckpointableConsumer>();
consumer.Subscribe<Event>("events", async (event, ctx) =>
{
    await ProcessEvent(event);
    await ctx.CheckpointAsync(); // blob checkpoint
});
```

## Features

### Service Bus

- **Publish**: queue or topic via `destination` parameter
- **Consume**: `ServiceBusProcessor` with session support
- **Options**: `SessionId` for session-enabled entities, `MaxConcurrentCalls` for parallelism
- **Ack/Nack**: `ctx.AckAsync()` or `ctx.NackAsync()` for message lifecycle

### Event Hubs

- **Publish**: event hub with partition key routing
- **Consume**: `EventProcessorClient` with blob checkpoint storage
- **Checkpoint**: `ctx.CheckpointAsync()` stores offset in Azure Blob Storage
- **Consumer Group**: required in `ConsumeOptions.ConsumerGroup`

### Raw Binary Support

Both providers support `[UBrokerRawBinary]` for zero-allocation struct serialization.

## Options Reference

### Service Bus Options

| Option | Type | Default | Description |
|---|---|---|---|
| `ConnectionString` | `string` | — | Service Bus connection string |

### Event Hubs Options

| Option | Type | Default | Description |
|---|---|---|---|
| `ConnectionString` | `string` | — | Event Hubs namespace connection string |
| `CheckpointStorageConnectionString` | `string` | — | Blob Storage connection string for checkpoints |

## Observability

- **Meter**: `ubroker.publish.messages`, `ubroker.consume.messages`
- **Tracing**: `traceparent` header propagated via broker-native properties
- **Serializer tag**: `"raw"` or `"json"` on all counters

## Requirements

- .NET 10.0 or later
- Azure.Messaging.ServiceBus 7.x
- Azure.Messaging.EventHubs 5.x
- Azure.Storage.Blobs 12.x (for Event Hubs checkpointing)
- Azure subscription with Service Bus / Event Hubs namespace

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [Azure Service Bus Documentation](https://learn.microsoft.com/azure/service-bus-messaging/)
- [Azure Event Hubs Documentation](https://learn.microsoft.com/azure/event-hubs/)
- [Report Issues](https://github.com/theunal/uBroker/issues)
