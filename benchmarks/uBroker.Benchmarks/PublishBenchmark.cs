using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using uBroker.RabbitMQ.Serialization;

namespace uBroker.Benchmarks;

/// <summary>
/// Benchmark comparing uBroker's zero-allocation serialization against
/// standard System.Text.Json serialization.
///
/// Key insight: at 50K msg/sec, even 200 bytes of allocation per message
/// equals 10 MB/sec of GC pressure. Zero-allocation eliminates this entirely.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PublishBenchmark
{
    /// <summary>1KB payload — representative of typical messaging workloads.</summary>
    private SampleMessage _message = default!;

    /// <summary>Reusable buffer writer for zero-allocation path.</summary>
    private ArrayBufferWriter<byte> _bufferWriter = default!;

    private Utf8JsonMessageSerializer _serializer = default!;

    [GlobalSetup]
    public void Setup()
    {
        _message = new SampleMessage
        {
            Id = 42,
            Content = new string('x', 900), // Pad to ~1KB after serialization
            Timestamp = DateTimeOffset.UtcNow,
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "benchmark",
                ["trace_id"] = Guid.NewGuid().ToString("D"),
            }
        };

        _bufferWriter = new ArrayBufferWriter<byte>(4096);
        _serializer = new Utf8JsonMessageSerializer();
    }

    /// <summary>
    /// Standard System.Text.Json serialization (allocates string + byte[]).
    /// This is what most libraries (MassTransit, raw RabbitMQ.Client) use.
    /// </summary>
    [Benchmark(Baseline = true)]
    public byte[] StandardSerialize()
    {
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_message);
    }

    /// <summary>
    /// uBroker zero-allocation serialization (writes into pooled buffer).
    /// Reuses the ArrayBufferWriter's internal buffer across calls.
    /// </summary>
    [Benchmark]
    public int ZeroAllocSerialize()
    {
        _bufferWriter.ResetWrittenCount();
        return _serializer.Serialize(_message, _bufferWriter);
    }

    /// <summary>
    /// Standard deserialization (allocates the message object + internal strings).
    /// </summary>
    [Benchmark]
    public SampleMessage StandardDeserialize()
    {
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_message);
        return System.Text.Json.JsonSerializer.Deserialize<SampleMessage>(bytes)!;
    }

    /// <summary>
    /// uBroker zero-allocation deserialization (reads from span, no intermediate string).
    /// </summary>
    [Benchmark]
    public SampleMessage ZeroAllocDeserialize()
    {
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_message);
        return _serializer.Deserialize<SampleMessage>(bytes);
    }
}

public sealed class SampleMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }
}
