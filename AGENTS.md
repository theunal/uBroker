# uBroker — Agent Guide

.NET 10 multi-broker messaging library. High-performance, zero-allocation.

## Build & Test

```powershell
# Build everything
dotnet build uBroker.slnx

# Run unit tests (xunit v3 + Moq)
dotnet test tests\uBroker.UnitTests

# Run integration tests (requires Testcontainers — Docker must be running)
dotnet test tests\uBroker.IntegrationTests

# Run benchmarks
dotnet run --project benchmarks\uBroker.Benchmarks

# Run samples (RabbitMQ only — set RABBITMQ_CONNECTION_STRING env var)
dotnet run --project samples\uBroker.Sample.Producer
dotnet run --project samples\uBroker.Sample.Consumer
```

## Project Map

| Path | Role |
|---|---|
| `src/uBroker.Core/` | Broker-agnostic interfaces (`IUBrokerPublisher`, `IUBrokerConsumer`, `IPartitionedPublisher`, `ICheckpointableConsumer`, `IMessageSerializer`), options, `MessageContext` |
| `src/uBroker.RabbitMQ/` | RabbitMQ via `RabbitMQ.Client` 7.x — channel pooling (`ObjectPool<IChannel>`), `BatchPublishWorker` (BackgroundService + `Channel<PublishRequest>`), Qos, DLX |
| `src/uBroker.Kafka/` | Kafka via `Confluent.Kafka` 2.x — implements `IPartitionedPublisher` + `ICheckpointableConsumer` |
| `src/uBroker.Azure/` | Azure Service Bus (`ServiceBusProcessor`) + Event Hubs (`EventProcessorClient` + blob checkpoint) |
| `src/uBroker.Aws/` | AWS SQS (`AmazonSQSClient`, long-poll, ack=DeleteMessage) + SNS (publish-only, `ISnsPublisher`) |
| `src/uBroker.Diagnostics/` | `Meter` ("uBroker") + `ActivitySource` for OpenTelemetry |
| `src/uBroker.DependencyInjection/` | `AddUBrokerRabbitMQ`, `AddUBrokerKafka`, `AddUBrokerAzureServiceBus`, `AddUBrokerAzureEventHubs`, `AddUBrokerAwsSqs`, `AddUBrokerAwsSns` |
| `tests/uBroker.UnitTests/` | Unit tests — RawBinary, WireFormat tests |
| `tests/uBroker.IntegrationTests/` | Integration tests — RabbitMQ, Kafka, Azure Service Bus, AWS SQS (Testcontainers) |
| `samples/uBroker.Sample.Producer/` | RabbitMQ producer sample |
| `samples/uBroker.Sample.Consumer/` | RabbitMQ consumer sample |
| `samples/uBroker.Sample.Kafka/` | Kafka producer+consumer sample |
| `samples/uBroker.Sample.Azure/` | Azure Service Bus sample |
| `samples/uBroker.Sample.Aws/` | AWS SQS sample |
| `benchmarks/uBroker.Benchmarks/` | BenchmarkDotNet — serialization + batching benchmarks |

## Architecture

- **Capability interfaces** avoid `NotSupportedException`: queue brokers (RabbitMQ, SQS) use `IUBrokerPublisher`/`IUBrokerConsumer`; log brokers (Kafka, Event Hubs) also implement `IPartitionedPublisher`/`ICheckpointableConsumer`; SNS is publish-only via `ISnsPublisher`.
- **All providers** target zero-allocation: `IMessageSerializer` uses `IBufferWriter<byte>` + `ReadOnlySpan<byte>` with `Utf8JsonReader`/`Utf8JsonWriter`.
- **`uBroker.Core`** exposes `InternalsVisibleTo` to RabbitMQ, Kafka, Azure, Aws, DependencyInjection, UnitTests, and Benchmarks.
- **`uBroker.DependencyInjection`** references all providers and all their NuGet packages directly — users only need one DI package.
- **Diagnostics** lives in `uBroker.Diagnostics` and depends only on Core. Meter counters: `messages_published`, `messages_consumed`, `batch_flush_reason`, `publish_errors`. Traceparent propagated through broker-native headers.

## Conventions

- Target: `net10.0`, `LangVersion` latest, nullable enabled, implicit usings.
- No `Directory.Build.props` — each csproj sets its own properties.
- No `.editorconfig` — formatting follows dotnet defaults.
- No `.sln` — solution is `uBroker.slnx` (new VS format).
- `ConsumeOptions.MaxConcurrentCalls` defaults to `Environment.ProcessorCount`.
- RabbitMQ `BatchPublishWorker`: flush at `BatchMaxSize` (500) or `BatchMaxWindow` (5ms), whichever hits first.
- Azure Event Hubs requires `CheckpointStorageConnectionString` (blob container) alongside the hub connection string.

## Local Infrastructure

`docker-compose.yml` provides all broker emulators for integration tests:

```powershell
# Start all services (RabbitMQ, Kafka KRaft, Azure SB emulator, Azurite, LocalStack)
docker compose up -d

# Tear down including volumes
docker compose down -v
```

| Service | Port | Notes |
|---|---|---|
| RabbitMQ | 5673 (AMQP), 15672 (UI) | Management UI at localhost:15672 |
| Kafka | 9092 | KRaft mode, no ZooKeeper |
| Azure SB Emulator | 5672 | Pre-configured queues/topics in `docker-compose-config/ServiceBusConfig.json` |
| Azurite | 10000-10002 | Blob/Queue/Table storage emulator |
| LocalStack | 4566 | SQS + SNS emulation |

`.env` provides secrets for docker-compose (`LOCALSTACK_AUTH_TOKEN`, `MSSQL_SA_PASSWORD`, `ACCEPT_EULA`). This file is gitignored — never commit it.

## Quirks & Gotchas

- **xunit v3**, not v2. Test projects use `xunit.v3` package directly (no `xunit` meta-package).
- **Moq** 4.* is available in unit tests.
- **Testcontainers** 4.* is available in integration tests — Docker must be running.
- The RabbitMQ serializer in `src/uBroker.RabbitMQ/Serialization/` is separate from the one in `src/uBroker.Azure/Internals/` — no shared serializer exists (each provider has its own copy).
- Kafka defaults: `EnableAutoCommit` = false (manual commit), `AutoOffsetReset` = "latest".
- SQS `MaxBatchSize` is capped at 10 (AWS API limit).
- SNS implements only `ISnsPublisher` — no consumer, no `IUBrokerConsumer`. SNS is publish-only (messages flow SNS → SQS).
