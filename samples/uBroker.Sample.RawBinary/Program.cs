using System.Runtime.InteropServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using uBroker;
using uBroker.DependencyInjection;

Console.WriteLine("uBroker Sample — Raw Binary vs JSON");
Console.WriteLine("=====================================");
Console.WriteLine();

// ── Setup RabbitMQ ──

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddUBrokerRabbitMQ(options =>
        {
            options.ConnectionString = Environment.GetEnvironmentVariable("RABBITMQ_CONNECTION_STRING")
                ?? "amqp://guest:guest@localhost:5673";
        });
    })
    .Build();

var publisher = host.Services.GetRequiredService<IUBrokerPublisher>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

const string exchange = "ubroker.rawbinary.sample";

// ── Publish raw binary struct ──

Console.WriteLine("Publishing 10,000 raw binary StockPrice structs...");
var sw = System.Diagnostics.Stopwatch.StartNew();

for (int i = 0; i < 10_000; i++)
{
    var msg = new StockPrice
    {
        ProductId = i,
        Price = 99.99m + i,
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    };
    await publisher.PublishAsync(exchange, msg,
        new PublishOptions { RoutingKey = "stock.raw" }, cts.Token);
}

var rawRate = 10_000 / sw.Elapsed.TotalSeconds;
Console.WriteLine($"  Raw binary: {sw.Elapsed.TotalSeconds:F2}s ({rawRate:N0} msg/sec)");

// ── Publish JSON class ──

Console.WriteLine("Publishing 10,000 JSON StockPriceJson objects...");
sw.Restart();

for (int i = 0; i < 10_000; i++)
{
    var msg = new StockPriceJson
    {
        ProductId = i,
        Price = 99.99m + i,
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    };
    await publisher.PublishAsync(exchange, msg,
        new PublishOptions { RoutingKey = "stock.json" }, cts.Token);
}

var jsonRate = 10_000 / sw.Elapsed.TotalSeconds;
Console.WriteLine($"  JSON:       {sw.Elapsed.TotalSeconds:F2}s ({jsonRate:N0} msg/sec)");

Console.WriteLine();
Console.WriteLine($"Raw binary is {jsonRate / rawRate:F1}x faster than JSON");
Console.WriteLine();
Console.WriteLine("Raw binary structs are automatically detected via [UBrokerRawBinary] attribute.");
Console.WriteLine("No API changes needed — same PublishAsync/Subscribe calls.");

// ── Type declarations (must come after top-level statements) ──

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct StockPrice
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}

public class StockPriceJson
{
    public int ProductId { get; set; }
    public decimal Price { get; set; }
    public long Timestamp { get; set; }
}
