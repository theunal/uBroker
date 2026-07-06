# uBroker.DependencyInjection

Dependency injection extensions for uBroker. One package to register all providers.

## Installation

```bash
dotnet add package uBroker.DependencyInjection
```

## Quick Start

```csharp
// Single provider
services.AddUBrokerRabbitMQ(o => o.ConnectionString = "...");

// Multiple providers
services.AddUBrokerRabbitMQ(o => o.ConnectionString = "...");
services.AddUBrokerKafka(o => o.BootstrapServers = "localhost:9092");
```

## Available Extensions

- `AddUBrokerRabbitMQ` — RabbitMQ
- `AddUBrokerKafka` — Kafka
- `AddUBrokerAzureServiceBus` — Azure Service Bus
- `AddUBrokerAzureEventHubs` — Azure Event Hubs
- `AddUBrokerAwsSqs` — AWS SQS
- `AddUBrokerAwsSns` — AWS SNS (publish-only)

## Links

- [GitHub](https://github.com/theunal/uBroker)
