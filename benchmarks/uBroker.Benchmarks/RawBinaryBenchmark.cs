using System.Buffers;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using uBroker;
using uBroker.RabbitMQ.Serialization;

namespace uBroker.Benchmarks;

[UBrokerRawBinary]
[StructLayout(LayoutKind.Sequential)]
public struct StockPriceEvent
{
    public int ProductId;
    public decimal Price;
    public long Timestamp;
}

/// <summary>
/// Compares raw binary serialization (MemoryMarshal blit) against JSON serialization
/// for a typical blittable messaging payload.
/// 
/// Key metrics:
/// - ns/op: raw path should be 10-50x faster
/// - allocated bytes/op: raw path should be 0 (after initial buffer), JSON path allocates strings
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class RawBinaryBenchmark
{
    private StockPriceEvent _message;
    private ArrayBufferWriter<byte> _bufferWriter = default!;
    private Utf8JsonMessageSerializer _jsonSerializer = default!;
    private byte[] _rawBuffer = default!;

    [GlobalSetup]
    public void Setup()
    {
        _message = new StockPriceEvent
        {
            ProductId = 42,
            Price = 99.99m,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        _bufferWriter = new ArrayBufferWriter<byte>(4096);
        _jsonSerializer = new Utf8JsonMessageSerializer();
        _rawBuffer = new byte[UnmanagedBlitSerializer.GetSize<StockPriceEvent>()];
    }

    [Benchmark(Baseline = true)]
    public int JsonSerialize()
    {
        _bufferWriter.ResetWrittenCount();
        return _jsonSerializer.Serialize(_message, _bufferWriter);
    }

    [Benchmark]
    public int RawSerialize()
    {
        UnmanagedBlitSerializer.Write(in _message, _rawBuffer);
        return _rawBuffer.Length;
    }

    [Benchmark]
    public StockPriceEvent JsonDeserialize()
    {
        _bufferWriter.ResetWrittenCount();
        var written = _jsonSerializer.Serialize(_message, _bufferWriter);
        return _jsonSerializer.Deserialize<StockPriceEvent>(
            _bufferWriter.WrittenSpan.Slice(0, written));
    }

    [Benchmark]
    public StockPriceEvent RawDeserialize()
    {
        UnmanagedBlitSerializer.Write(in _message, _rawBuffer);
        return UnmanagedBlitSerializer.Read<StockPriceEvent>(_rawBuffer);
    }
}
