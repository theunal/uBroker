# uBroker.Aws

AWS SQS and SNS implementation for uBroker.

## Installation

```bash
dotnet add package uBroker.Aws
```

## Overview

This package provides two AWS messaging providers:

- **AWS SQS**: Queue-based publish/consume with long-polling, visibility timeout, and FIFO support
- **AWS SNS**: Publish-only fan-out to SQS topics (no consumer — SNS → SQS pattern)

## Quick Start

### SQS

```csharp
using Microsoft.Extensions.DependencyInjection;
using uBroker;
using uBroker.Aws.Sqs;

// Register
services.AddUBrokerAwsSqs(o =>
{
    o.Region = "eu-central-1";
    o.ServiceURL = "http://localhost:4566"; // LocalStack for dev
    o.WaitTimeSeconds = 20; // long-poll
});

// Publish
var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("https://sqs.eu-central-1.amazonaws.com/123456789/orders",
    new Order { Id = 1 });

// Consume
var consumer = sp.GetRequiredService<IUBrokerConsumer>();
consumer.Subscribe<Order>("https://sqs.eu-central-1.amazonaws.com/123456789/orders",
    async (order, ctx) =>
    {
        await ProcessOrder(order);
        await ctx.AckAsync(); // DeleteMessage
    });
```

### SNS (Publish-Only)

```csharp
using uBroker.Aws.Sns;

// Register
services.AddUBrokerAwsSns(o => o.Region = "eu-central-1");

// Publish to SNS topic (fan-out to subscribed SQS queues)
var snsPublisher = sp.GetRequiredService<ISnsPublisher>();
await snsPublisher.PublishAsync("arn:aws:sns:eu-central-1:123456789:orders",
    new Order { Id = 1 });
```

## Features

### SQS

- **Long-polling**: configurable `WaitTimeSeconds` (default: 20)
- **Ack**: `ctx.AckAsync()` calls `DeleteMessage`
- **Visibility Timeout**: configurable via `ConsumeOptions.VisibilityTimeoutSeconds`
- **Batching**: `SendMessageBatch` (max 10 messages per batch, AWS API limit)
- **FIFO**: support via `PublishOptions.MessageGroupId` and `DeduplicationId`

### SNS

- **Publish-only**: `ISnsPublisher` interface (no `IUBrokerConsumer`)
- **Fan-out pattern**: SNS topic → SQS queue subscription
- **Message Attributes**: headers propagated as SNS `MessageAttributeValue`
- **FIFO**: support via `PublishOptions.MessageGroupId`

### Raw Binary Support

Both providers support `[UBrokerRawBinary]` for zero-allocation struct serialization.

## Options Reference

### SQS Options

| Option | Type | Default | Description |
|---|---|---|---|
| `Region` | `string` | `us-east-1` | AWS region |
| `ServiceURL` | `string?` | `null` | Custom endpoint (e.g., LocalStack) |
| `AccessKey` | `string?` | `null` | AWS access key (optional, uses SDK chain) |
| `SecretKey` | `string?` | `null` | AWS secret key (optional) |
| `WaitTimeSeconds` | `int` | `20` | Long-poll wait time |

### SNS Options

| Option | Type | Default | Description |
|---|---|---|---|
| `Region` | `string` | `us-east-1` | AWS region |
| `ServiceURL` | `string?` | `null` | Custom endpoint (e.g., LocalStack) |
| `AccessKey` | `string?` | `null` | AWS access key (optional) |
| `SecretKey` | `string?` | `null` | AWS secret key (optional) |

## Local Development

Use [LocalStack](https://localstack.cloud/) for local development:

```bash
# Start LocalStack
docker run -d -p 4566:4566 localstack/localstack

# Create queue
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name orders

# Create SNS topic
aws --endpoint-url=http://localhost:4566 sns create-topic --name orders
```

## Observability

- **Meter**: `ubroker.publish.messages`, `ubroker.consume.messages`
- **Tracing**: `traceparent` header propagated via message attributes
- **Serializer tag**: `"raw"` or `"json"` on all counters

## Requirements

- .NET 10.0 or later
- AWSSDK.SQS 4.x
- AWSSDK.SimpleNotificationService 4.x
- AWS account (or LocalStack for development)

## Links

- [GitHub Repository](https://github.com/theunal/uBroker)
- [AWS SQS Documentation](https://docs.aws.amazon.com/sqs/)
- [AWS SNS Documentation](https://docs.aws.amazon.com/sns/)
- [Report Issues](https://github.com/theunal/uBroker/issues)
