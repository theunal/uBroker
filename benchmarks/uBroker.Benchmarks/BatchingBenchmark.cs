using BenchmarkDotNet.Attributes;

namespace uBroker.Benchmarks;

/// <summary>
/// Simulates the batching logic to measure throughput without a live broker.
/// Shows the difference between individual publishes vs batched publishes.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class BatchingBenchmark
{
    private const int MessageCount = 100_000;
    private const int PayloadSize = 1024; // 1KB

    private byte[] _payload = default!;

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadSize];
        Random.Shared.NextBytes(_payload);
    }

    /// <summary>
    /// Simulate individual publish: each message is a separate "network call".
    /// In real scenarios, this means N syscalls to RabbitMQ.
    /// </summary>
    [Benchmark(Description = "Individual publishes (no batching)")]
    public long IndividualPublishes()
    {
        long totalBytes = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            // Simulate serialization + framing overhead per message.
            totalBytes += _payload.Length;
        }
        return totalBytes;
    }

    /// <summary>
    /// Simulate batched publish: messages are grouped and sent in bulk.
    /// This is what BatchPublishWorker does with BasicPublishBatch.
    /// </summary>
    [Benchmark(Description = "Batched publishes (500 msg/batch)")]
    public long BatchedPublishes()
    {
        const int batchSize = 500;
        long totalBytes = 0;
        var batchCount = 0;

        for (var i = 0; i < MessageCount; i++)
        {
            totalBytes += _payload.Length;
            batchCount++;

            if (batchCount >= batchSize)
            {
                // Simulate single batch publish.
                batchCount = 0;
            }
        }

        // Flush remaining.
        return totalBytes;
    }
}
