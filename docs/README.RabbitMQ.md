# uBroker.RabbitMQ

RabbitMQ implementation for uBroker. Channel pooling, batch publishing, zero-allocation.

## Installation

```bash
dotnet add package uBroker.RabbitMQ
```

## Quick Start

```csharp
services.AddUBrokerRabbitMQ(o =>
{
    o.ConnectionString = "amqp://guest:guest@localhost:5672";
    o.BatchMaxSize = 500;
    o.BatchMaxWindow = TimeSpan.FromMilliseconds(5);
});

var publisher = sp.GetRequiredService<IUBrokerPublisher>();
await publisher.PublishAsync("exchange", message, new PublishOptions { RoutingKey = "key" });
```

## Features

- Channel pooling via `ObjectPool<IChannel>`
- Batch publish worker (BackgroundService + `Channel<PublishRequest>`)
- Flush at `BatchMaxSize` (500) or `BatchMaxWindow` (5ms), whichever hits first
- Raw binary support via `[UBrokerRawBinary]`

## Links

- [GitHub](https://github.com/theunal/uBroker)
