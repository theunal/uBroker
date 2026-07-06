# uBroker — Agent Guide

.NET 10 multi-broker messaging library. High-performance, zero-allocation.

## Build & Test

```powershell
# Build everything
dotnet build uBroker.slnx

# Run unit tests (xunit v3 + Moq)
dotnet test tests\uBroker.UnitTests

# Run integration tests (requires Testcontainers — Docker needed)
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
| `tests/uBroker.UnitTests/` | Unit tests — has csproj only (no test files yet) |
| `tests/uBroker.IntegrationTests/` | Integration tests — Testcontainers, has csproj only (no test files yet) |
| `samples/uBroker.Sample.Producer/` | RabbitMQ producer sample (working) |
| `samples/uBroker.Sample.Consumer/` | RabbitMQ consumer sample (working) |
| `samples/uBroker.Sample.Aws/` | Scaffold only (csproj, no source) |
| `samples/uBroker.Sample.Azure/` | Scaffold only (csproj, no source) |
| `samples/uBroker.Sample.Kafka/` | Scaffold only (csproj, no source) |
| `benchmarks/uBroker.Benchmarks/` | BenchmarkDotNet — serialization + batching benchmarks |

## Architecture

- **Capability interfaces** avoid `NotSupportedException`: queue brokers (RabbitMQ, SQS) use `IUBrokerPublisher`/`IUBrokerConsumer`; log brokers (Kafka, Event Hubs) also implement `IPartitionedPublisher`/`ICheckpointableConsumer`; SNS is publish-only via `ISnsPublisher`.
- **All providers** target zero-allocation: `IMessageSerializer` uses `IBufferWriter<byte>` + `ReadOnlySpan<byte>` with `Utf8JsonReader`/`Utf8JsonWriter`.
- **`uBroker.Core`** exposes `InternalsVisibleTo` to RabbitMQ, Kafka, Azure, Aws, and DependencyInjection.
- **`uBroker.DependencyInjection`** references all providers and all their NuGet packages directly — users only need one DI package.
- **Diagnostics** lives in `uBroker.Diagnostics` and depends only on Core. Meter counters: `messages_published`, `messages_consumed`, `batch_flush_reason`, `publish_errors`. Traceparent propagated through broker-native headers.

## Conventions

- Target: `net10.0`, `LangVersion` latest, nullable enabled, implicit usings.
- No `Directory.Build.props` — each csproj sets its own properties.
- No `.editorconfig` — formatting follows dotnet defaults.
- No `.sln` — solution is `uBroker.slnx` (new VS format).
- No git repo initialized — `.gitignore` does not exist yet.
- `ConsumeOptions.MaxConcurrentCalls` defaults to `Environment.ProcessorCount`.
- RabbitMQ `BatchPublishWorker`: flush at `BatchMaxSize` (500) or `BatchMaxWindow` (5ms), whichever hits first.
- Azure Event Hubs requires `CheckpointStorageConnectionString` (blob container) alongside the hub connection string.

## Quirks & Gotchas

- **xunit v3**, not v2. Test projects use `xunit.v3` package directly (no `xunit` meta-package).
- **Moq** 4.* is available in unit tests.
- **Testcontainers** 4.* is available in integration tests — Docker must be running.
- 3 sample projects and both test projects have **only csproj scaffolding** — source files need to be created.
- The RabbitMQ serializer in `src/uBroker.RabbitMQ/Serialization/` is separate from the one in `src/uBroker.Azure/Internals/` — no shared serializer exists (each provider has its own copy).
- Kafka defaults: `EnableAutoCommit` = false (manual commit), `AutoOffsetReset` = "latest".
- SQS `MaxBatchSize` is capped at 10 (AWS API limit).
- SNS implements only `ISnsPublisher` — no consumer, no `IUBrokerConsumer`. SNS is publish-only (messages flow SNS → SQS).
