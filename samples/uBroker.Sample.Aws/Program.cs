using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;
using uBroker.Aws.Sqs;

Console.WriteLine("uBroker Sample — AWS SQS Producer + Consumer");
Console.WriteLine("=============================================");

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerAwsSqs(options =>
        {
            options.Region = Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-central-1";
            options.ServiceURL = Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
            options.AccessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY") ?? "test";
            options.SecretKey = Environment.GetEnvironmentVariable("AWS_SECRET_KEY") ?? "test";
            options.WaitTimeSeconds = 2;
            options.DefaultVisibilityTimeoutSeconds = 30;
        });
    })
    .Build();

var publisher = host.Services.GetRequiredService<IUBrokerPublisher>();
var consumer = host.Services.GetRequiredService<IUBrokerConsumer>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var queueUrl = Environment.GetEnvironmentVariable("SQS_QUEUE_URL")
    ?? "http://localhost:4566/000000000000/ubroker-sample";

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
            Content = $"SQS message #{count}",
            Timestamp = DateTimeOffset.UtcNow,
        };

        // For FIFO queues, add MessageGroupId:
        await publisher.PublishAsync(queueUrl, message,
            new PublishOptions
            {
                // MessageGroupId = "group-1",        // Uncomment for FIFO queues
                // DeduplicationId = $"dedup-{count}", // Uncomment for FIFO queues
                Headers = new Dictionary<string, object> { ["source"] = "sample" },
            },
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

consumer.Subscribe<SampleMessage>(queueUrl, async (message, ctx) =>
{
    consumedCount++;
    if (consumedCount % 100 == 0)
    {
        var rate = consumedCount / consumeStopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"[Consumer] {consumedCount:N0} messages ({rate:N0} msg/sec)");
    }
    // SQS: delete message (ack)
    await ctx.AckAsync();
}, new ConsumeOptions
{
    MaxConcurrentCalls = 4,
    WaitTimeSeconds = 2,
    VisibilityTimeoutSeconds = 30,
});

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
