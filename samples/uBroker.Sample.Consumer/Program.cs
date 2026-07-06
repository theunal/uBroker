using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;

Console.WriteLine("uBroker Sample Consumer (RabbitMQ)");
Console.WriteLine("===================================");
Console.WriteLine("Consuming messages from RabbitMQ...");
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerRabbitMQ(options =>
        {
            options.ConnectionString = Environment.GetEnvironmentVariable("RABBITMQ_CONNECTION_STRING")
                ?? "amqp://guest:guest@localhost:5673";
            options.PrefetchCount = 250;
            options.Concurrency = Environment.ProcessorCount;
        });
    })
    .Build();

var consumer = host.Services.GetRequiredService<IUBrokerConsumer>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var messageCount = 0;
var stopwatch = System.Diagnostics.Stopwatch.StartNew();

var subscription = consumer.Subscribe<SampleMessage>(
    destination: "ubroker.sample.queue",
    handler: async (message, context) =>
    {
        await Task.Delay(1);
        messageCount++;

        if (messageCount % 1000 == 0)
        {
            var rate = messageCount / stopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"Consumed {messageCount:N0} messages ({rate:N0} msg/sec)");
        }

        await context.AckAsync();
    });

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException) { }

Console.WriteLine();
Console.WriteLine($"Total: {messageCount:N0} messages in {stopwatch.Elapsed.TotalSeconds:F2}s");
Console.WriteLine($"Average: {messageCount / stopwatch.Elapsed.TotalSeconds:N0} msg/sec");

subscription.Dispose();

public sealed class SampleMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
}
