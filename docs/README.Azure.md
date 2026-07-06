# uBroker.Azure

Azure Service Bus + Event Hubs implementation for uBroker.

## Installation

```bash
dotnet add package uBroker.Azure
```

## Quick Start

```csharp
// Service Bus
services.AddUBrokerAzureServiceBus(o => o.ConnectionString = "...");

// Event Hubs
services.AddUBrokerAzureEventHubs(o =>
{
    o.ConnectionString = "...";
    o.CheckpointStorageConnectionString = "...";
});
```

## Features

- **Service Bus**: queue/topic publish, processor + session consume
- **Event Hubs**: partitioned publish, `EventProcessorClient` + blob checkpoint
- Raw binary support via `[UBrokerRawBinary]`

## Links

- [GitHub](https://github.com/theunal/uBroker)
