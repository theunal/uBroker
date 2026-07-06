# uBroker.DependencyInjection

Dependency injection extensions for uBroker. Register providers with a single package.

## Installation

```bash
dotnet add package uBroker.DependencyInjection
```

## Overview

`uBroker.DependencyInjection` references all uBroker provider packages and their NuGet dependencies. Users only need this one package to register any combination of brokers via DI.

## Quick Start

```csharp
using Microsoft.Extensions.DependencyInjection;
using uBroker.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Single provider
services.AddUBrokerRabbitMQ(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672";
});

// Multiple providers
services.AddUBrokerRabbitMQ(o => o.ConnectionString = "...");
services.AddUBrokerKafka(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.ConsumerGroup = "my-service";
});

var app = builder.Build();
```

## Available Extensions

### RabbitMQ

```csharp
services.AddUBrokerRabbitMQ(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672";
    o.BatchMaxSize = 500;
    o.BatchMaxWindow = TimeSpan.FromMilliseconds(5);
    o.ChannelPoolSize = 16;
    o.PrefetchCount = 50;
});
```

### Kafka

```csharp
services.AddUBrokerKafka(o =>
{
    o.BootstrapServers = "localhost:9092";
    o.ConsumerGroup = "my-service";
    o.LingerMs = 5;
    o.BatchSize = 16384;
    o.CompressionType = "none";
    o.Acks = "all";
});
```

### Azure Service Bus

```csharp
services.AddUBrokerAzureServiceBus(o =>
{
    o.ConnectionString = "Endpoint=sb://your-ns.servicebus.windows.net/;...";
});
```

### Azure Event Hubs

```csharp
services.AddUBrokerAzureEventHubs(o =>
{
    o.ConnectionString = "Endpoint=sb://your-ns.servicebus.windows.net/;...";
    o.CheckpointStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=...";
});
```

### AWS SQS

```csharp
services.AddUBrokerAwsSqs(o =>
{
    o.Region = "eu-central-1";
    o.ServiceURL = "http://localhost:4566"; // LocalStack
    o.WaitTimeSeconds = 20;
});
```

### AWS SNS (Publish-Only)

```csharp
services.AddUBrokerAwsSns(o =>
{
    o.Region = "eu-central-1";
    o.ServiceURL = "http://localhost:4566"; // LocalStack
});
```

## Keyed Services (Multiple Providers)

When using multiple providers simultaneously, use keyed services:

```csharp
services.AddKeyedSingleton<IUBrokerPublisher>("rabbitmq", (sp, _) =>
{
    // RabbitMQ publisher
});

services.AddKeyedSingleton<IUBrokerPublisher>("kafka", (sp, _) =>
{
    // Kafka publisher
});

// Inject with attribute
public class OrderService(
    [FromKeyedServices("rabbitmq")] IUBrokerPublisher rabbitPublisher,
    [FromKeyedServices("kafka")] IUBrokerPublisher kafkaPublisher)
{
    // Use both publishers
}
```

## What Each Extension Registers

| Extension | Registers |
|---|---|
| `AddUBrokerRabbitMQ` | `IConnection`, `ObjectPool<IChannel>`, `IChannelPool`, `BatchPublishWorker`, `IUBrokerPublisher`, `IUBrokerConsumer` |
| `AddUBrokerKafka` | `IProducer<string, byte[]>`, `IUBrokerPublisher`, `ICheckpointableConsumer`, `IPartitionedPublisher` |
| `AddUBrokerAzureServiceBus` | `ServiceBusClient`, `ServiceBusSender`, `IUBrokerPublisher`, `IUBrokerConsumer` |
| `AddUBrokerAzureEventHubs` | `EventHubProducerClient`, `IUBrokerPublisher`, `ICheckpointableConsumer`, `IPartitionedPublisher` |
| `AddUBrokerAwsSqs` | `AmazonSQSClient`, `IUBrokerPublisher`, `IUBrokerConsumer` |
| `AddUBrokerAwsSns` | `AmazonSimpleNotificationServiceClient`, `ISnsPublisher` |

All extensions also register `UBrokerDiagnostics` and `IMessageSerializer` where applicable.

## Requirements

- .NET 10.0 or later
- References all uBroker provider packages and their NuGet dependencies

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [Report Issues](https://github.com/theunal/uBroker/issues)
