using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using uBroker;
using uBroker.Azure.Internals;
using uBroker.Azure.ServiceBus;
using uBroker.Diagnostics;
using Xunit;

namespace uBroker.IntegrationTests;

/// <summary>
/// Integration tests for Azure Service Bus provider.
/// Requires: docker compose up -d (Service Bus Emulator on port 5672)
/// </summary>
public class AzureServiceBusIntegrationTests : IAsyncLifetime
{
    private ServiceBusClient? _client;
    private ServiceBusSender? _sender;
    private AzureServiceBusPublisher? _publisher;
    private AzureServiceBusConsumer? _consumer;

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING")
        ?? "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    public ValueTask InitializeAsync()
    {
        _client = new ServiceBusClient(ConnectionString);
        _sender = _client.CreateSender("default");

        var diagnostics = new UBrokerDiagnostics();

        _publisher = new AzureServiceBusPublisher(
            _sender,
            new Utf8JsonMessageSerializer(),
            diagnostics,
            NullLogger<AzureServiceBusPublisher>.Instance);

        _consumer = new AzureServiceBusConsumer(
            _client,
            diagnostics,
            NullLogger<AzureServiceBusConsumer>.Instance);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _publisher?.Dispose();
        if (_client is not null)
        {
            try { await _client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3)); }
            catch { }
        }
    }

    [Fact]
    public async Task PublishAsync_ShouldNotThrow()
    {
        var message = new SbTestMessage { Id = 1, Content = "hello" };
        await _publisher!.PublishAsync("default", message);
    }

    [Fact]
    public async Task PublishWithHeaders_ShouldWork()
    {
        var message = new SbTestMessage { Id = 1, Content = "with-headers" };
        await _publisher!.PublishAsync("default", message,
            new PublishOptions
            {
                Headers = new Dictionary<string, object>
                {
                    ["custom-header"] = "custom-value",
                }
            });
    }

    [Fact]
    public async Task PublishAndConsume_ShouldReceiveMessage()
    {
        var received = new TaskCompletionSource<SbTestMessage>();

        using var sub = _consumer!.Subscribe<SbTestMessage>("ubroker.sample", async (msg, ctx) =>
        {
            received.TrySetResult(msg);
            await ctx.AckAsync();
        }, new ConsumeOptions { MaxConcurrentCalls = 1 });

        await Task.Delay(2000);

        var message = new SbTestMessage { Id = 42, Content = "sb-integration" };
        await _publisher!.PublishAsync("default", message);
    }
}

public class SbTestMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
}
