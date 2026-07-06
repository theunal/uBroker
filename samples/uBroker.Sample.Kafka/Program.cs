using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;

Console.WriteLine("uBroker Sample — Kafka Producer + Consumer");
Console.WriteLine("===========================================");

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerKafka(options =>
        {
            options.BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")
                ?? "localhost:9092";
            options.ConsumerGroup = "ubroker-sample";
            options.LingerMs = 5;
            options.BatchSize = 16384;
            options.CompressionType = "none";
        });
    })
    .Build();

// ── Publisher ──
var publisher = host.Services.GetRequiredService<IUBrokerPublisher>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

const string topic = "ubroker.sample";
var stopwatch = System.Diagnostics.Stopwatch.StartNew();
var messageCount = 0;

_ = Task.Run(async () =>
{
    while (!cts.Token.IsCancellationRequested)
    {
        var message = new SampleMessage
        {
            Id = messageCount,
            Content = $"Kafka message #{messageCount}",
            Timestamp = DateTimeOffset.UtcNow,
        };

        await publisher.PublishAsync(topic, message,
            new PublishOptions { PartitionKey = $"key-{messageCount % 4}" }, cts.Token);
        messageCount++;

        if (messageCount % 1000 == 0)
        {
            var rate = messageCount / stopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"[Producer] {messageCount:N0} messages ({rate:N0} msg/sec)");
        }
    }
}, cts.Token);

// ── Consumer ──
var consumer = host.Services.GetRequiredService<IUBrokerConsumer>();
var consumedCount = 0;
var consumeStopwatch = System.Diagnostics.Stopwatch.StartNew();

consumer.Subscribe<SampleMessage>(topic, async (message, ctx) =>
{
    consumedCount++;
    if (consumedCount % 1000 == 0)
    {
        var rate = consumedCount / consumeStopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"[Consumer] {consumedCount:N0} messages ({rate:N0} msg/sec)");
    }
    // Kafka: checkpoint offset
    await ctx.CheckpointAsync();
}, new ConsumeOptions { ConsumerGroup = "ubroker-sample-group" });

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException) { }

Console.WriteLine();
Console.WriteLine($"Published: {messageCount:N0} | Consumed: {consumedCount:N0}");

public sealed class SampleMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
}
