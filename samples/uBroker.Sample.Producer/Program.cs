using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;

Console.WriteLine("uBroker Sample Producer (RabbitMQ)");
Console.WriteLine("===================================");
Console.WriteLine("Publishing messages to RabbitMQ...");
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerRabbitMQ(options =>
        {
            options.ConnectionString = Environment.GetEnvironmentVariable("RABBITMQ_CONNECTION_STRING")
                ?? "amqp://guest:guest@localhost:5673";
            options.BatchMaxSize = 500;
            options.BatchMaxWindow = TimeSpan.FromMilliseconds(5);
        });
    })
    .Build();

var publisher = host.Services.GetRequiredService<IUBrokerPublisher>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
var messageCount = 0;
const string exchange = "ubroker.sample";
const string routingKey = "sample.message";

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var message = new SampleMessage(messageCount, DateTimeOffset.UtcNow)
        {
            Content = $"Hello from uBroker! Message #{messageCount}",
        };

        await publisher.PublishAsync(exchange, message,
            new PublishOptions { RoutingKey = routingKey }, cts.Token);
        messageCount++;

        if (messageCount % 1000 == 0)
        {
            var rate = messageCount / stopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"Published {messageCount:N0} messages ({rate:N0} msg/sec)");
        }
    }
}
catch (OperationCanceledException) { }

Console.WriteLine();
Console.WriteLine($"Total: {messageCount:N0} messages in {stopwatch.Elapsed.TotalSeconds:F2}s");
Console.WriteLine($"Average: {messageCount / stopwatch.Elapsed.TotalSeconds:N0} msg/sec");

public sealed record SampleMessage(int Id, DateTimeOffset Timestamp)
{
    public string Content { get; set; } = default!;
}
