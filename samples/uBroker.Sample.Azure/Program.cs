using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;

Console.WriteLine("uBroker Sample — Azure Service Bus Producer + Consumer");
Console.WriteLine("======================================================");
Console.WriteLine("Uses Service Bus Emulator (docker-compose up -d)");
Console.WriteLine();

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerAzureServiceBus(options =>
        {
            options.ConnectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING")
                ?? "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        });
    })
    .Build();

var publisher = host.Services.GetRequiredService<IUBrokerPublisher>();
var consumer = host.Services.GetRequiredService<IUBrokerConsumer>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

const string queueName = "ubroker-sample";

// ── Publisher ──
_ = Task.Run(async () =>
{
    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    var count = 0;
    while (!cts.Token.IsCancellationRequested)
    {
        var message = new SampleMessage
        {
            Id = count,
            Content = $"Service Bus message #{count}",
            Timestamp = DateTimeOffset.UtcNow,
        };

        await publisher.PublishAsync(queueName, message,
            new PublishOptions { Headers = new Dictionary<string, object> { ["source"] = "sample" } },
            cts.Token);
        count++;

        if (count % 100 == 0)
        {
            var rate = count / stopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"[Producer] {count:N0} messages ({rate:N0} msg/sec)");
        }
    }
}, cts.Token);

// ── Consumer ──
var consumedCount = 0;
var consumeStopwatch = System.Diagnostics.Stopwatch.StartNew();

consumer.Subscribe<SampleMessage>(queueName, async (message, ctx) =>
{
    consumedCount++;
    if (consumedCount % 100 == 0)
    {
        var rate = consumedCount / consumeStopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"[Consumer] {consumedCount:N0} messages ({rate:N0} msg/sec)");
    }
    // Service Bus: complete (ack) the message
    await ctx.AckAsync();
}, new ConsumeOptions { MaxConcurrentCalls = 4 });

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException) { }

Console.WriteLine();
Console.WriteLine("Done.");

public sealed class SampleMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
}
