using System.Buffers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using uBroker;
using Xunit;

namespace uBroker.IntegrationTests;

/// <summary>
/// Integration tests for RabbitMQ provider.
/// Uses raw RabbitMQ.Client directly (no DI, no BatchPublishWorker) to avoid disposal hangs.
/// Requires: docker compose up -d (RabbitMQ on port 5673)
/// </summary>
public class RabbitMqIntegrationTests : IAsyncLifetime
{
    private IConnection? _connection;

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable("RABBITMQ_CONNECTION_STRING")
        ?? "amqp://guest:guest@localhost:5673";

    public async ValueTask InitializeAsync()
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(ConnectionString),
            AutomaticRecoveryEnabled = true,
        };
        _connection = await factory.CreateConnectionAsync();
    }

    public ValueTask DisposeAsync()
    {
        if (_connection is { IsOpen: true })
        {
            _connection.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PublishAndConsume_ShouldReceiveMessage()
    {
        var queue = $"test.queue.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<string>();

        // Consumer: declare queue, consume.
        var consumerChannel = await _connection!.CreateChannelAsync();
        await consumerChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var body = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
            received.TrySetResult(body);
            await consumerChannel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };
        await consumerChannel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);

        // Publisher: publish to default exchange with queue name as routing key.
        var publisherChannel = await _connection.CreateChannelAsync();
        var props = new BasicProperties { DeliveryMode = DeliveryModes.Persistent };
        var body = System.Text.Encoding.UTF8.GetBytes("hello-rabbitmq");
        await publisherChannel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: false, basicProperties: props, body: body);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("hello-rabbitmq", result);

        await consumerChannel.CloseAsync();
        await publisherChannel.CloseAsync();
    }

    [Fact]
    public async Task PublishMultiple_ShouldReceiveAll()
    {
        var queue = $"test.queue.{Guid.NewGuid():N}";
        var received = new List<string>();
        var allReceived = new TaskCompletionSource();

        var consumerChannel = await _connection!.CreateChannelAsync();
        await consumerChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var body = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
            lock (received) { received.Add(body); }
            if (received.Count >= 5) allReceived.TrySetResult();
            await consumerChannel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };
        await consumerChannel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);

        var publisherChannel = await _connection.CreateChannelAsync();
        for (int i = 0; i < 5; i++)
        {
            var body = System.Text.Encoding.UTF8.GetBytes($"msg-{i}");
            await publisherChannel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: false, body: body);
        }

        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(5, received.Count);

        await consumerChannel.CloseAsync();
        await publisherChannel.CloseAsync();
    }

    [Fact]
    public async Task PublishRawBinary_ShouldReceiveCorrectly()
    {
        var queue = $"test.queue.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<byte[]>();

        var consumerChannel = await _connection!.CreateChannelAsync();
        await consumerChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            received.TrySetResult(ea.Body.ToArray());
            await consumerChannel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };
        await consumerChannel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);

        // Simulate raw binary: struct blit to byte[]
        var original = new StockPrice { ProductId = 100, Price = 25.50m, Timestamp = 1700000000L };
        var size = System.Runtime.InteropServices.Marshal.SizeOf<StockPrice>();
        var buffer = new byte[size];
        System.Runtime.InteropServices.MemoryMarshal.Write(buffer, in original);

        var publisherChannel = await _connection.CreateChannelAsync();
        await publisherChannel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: false, body: buffer);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(size, result.Length);

        // Deserialize and verify.
        var deserialized = System.Runtime.InteropServices.MemoryMarshal.Read<StockPrice>(result);
        Assert.Equal(100, deserialized.ProductId);
        Assert.Equal(25.50m, deserialized.Price);
        Assert.Equal(1700000000L, deserialized.Timestamp);

        await consumerChannel.CloseAsync();
        await publisherChannel.CloseAsync();
    }
}

[UBrokerRawBinary]
[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
public struct StockPrice
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}
