using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using uBroker.Diagnostics;
using uBroker.Kafka;
using uBroker.Kafka.Serialization;
using Xunit;

namespace uBroker.IntegrationTests;

public class KafkaIntegrationTests : IAsyncLifetime
{
    private KafkaPublisher? _publisher;
    private KafkaConsumer? _consumer;
    private static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";

    public ValueTask InitializeAsync()
    {
        var kafkaOptions = new KafkaOptions
        {
            BootstrapServers = BootstrapServers,
            ConsumerGroup = $"test-{Guid.NewGuid():N}",
            LingerMs = 5,
            BatchSize = 16384
        };
        var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            LingerMs = 5,
            BatchSize = 16384
        }).Build();
        var serializer = new Utf8JsonMessageSerializer();
        var diag = new UBrokerDiagnostics();
        _publisher = new KafkaPublisher(producer, serializer, diag, NullLogger<KafkaPublisher>.Instance);
        _consumer = new KafkaConsumer(Options.Create(kafkaOptions), diag, NullLogger<KafkaConsumer>.Instance);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _publisher?.Dispose();
        _consumer?.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PublishAsync_ShouldNotThrow()
    {
        var topic = $"test-{Guid.NewGuid():N}";
        await _publisher!.PublishAsync(topic, new KafkaTestMessage { Id = 1, Content = "hello" }, null, CancellationToken.None);
    }

    [Fact]
    public async Task PublishAndConsume_ShouldReceiveMessage()
    {
        var topic = $"test-{Guid.NewGuid():N}";
        var message = new KafkaTestMessage { Id = 42, Content = "kafka-integration" };
        await _publisher!.PublishAsync(topic, message, null, CancellationToken.None);

        // Consume directly with Confluent.Kafka to verify round-trip.
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"test-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        Assert.NotNull(result);
        Assert.Contains("kafka-integration", result.Message.Value);
    }

    [Fact]
    public async Task PublishWithPartitionKey_ShouldWork()
    {
        var topic = $"test-{Guid.NewGuid():N}";
        await _publisher!.PublishAsync(topic, new KafkaTestMessage { Id = 1, Value = "partitioned" },
            new PublishOptions { PartitionKey = "key-1" }, CancellationToken.None);
    }
}

public class KafkaTestMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
    public string? Value { get; set; }
}
