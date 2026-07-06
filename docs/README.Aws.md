# uBroker.Aws

AWS SQS + SNS implementation for uBroker.

## Installation

```bash
dotnet add package uBroker.Aws
```

## Quick Start

```csharp
// SQS
services.AddUBrokerAwsSqs(o =>
{
    o.Region = "eu-central-1";
    o.ServiceURL = "http://localhost:4566"; // LocalStack
});

// SNS (publish-only)
services.AddUBrokerAwsSns(o => o.Region = "eu-central-1");
```

## Features

- **SQS**: long-poll consume, `DeleteMessage` ack, `SendMessageBatch` (max 10)
- **SNS**: publish-only via `ISnsPublisher`, SNS → SQS fan-out pattern
- FIFO support via `MessageGroupId`
- Raw binary support via `[UBrokerRawBinary]`

## Links

- [GitHub](https://github.com/theunal/uBroker)
