namespace uBroker.Kafka;

/// <summary>
/// Confluent.Kafka-specific configuration options.
/// 
/// Why separate from Core: Core is broker-agnostic. Each provider owns its options.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>Bootstrap servers (e.g. "localhost:9092").</summary>
    public string BootstrapServers { get; set; } = default!;

    /// <summary>Default topic for publishing (can be overridden per-message).</summary>
    public string? DefaultTopic { get; set; }

    /// <summary>Producer: linger.ms — time to wait for additional messages before sending a batch.</summary>
    public int LingerMs { get; set; } = 5;

    /// <summary>Producer: batch.size — maximum batch size in bytes.</summary>
    public int BatchSize { get; set; } = 16384;

    /// <summary>Producer: compression type (none, gzip, snappy, lz4, zstd).</summary>
    public string CompressionType { get; set; } = "none";

    /// <summary>Producer: acks (all, leader, none).</summary>
    public string Acks { get; set; } = "leader";

    /// <summary>Consumer: default consumer group.</summary>
    public string ConsumerGroup { get; set; } = "ubroker";

    /// <summary>Consumer: enable auto commit (false recommended for manual checkpoint).</summary>
    public bool EnableAutoCommit { get; set; } = false;

    /// <summary>Consumer: auto offset reset policy (earliest, latest, error).</summary>
    public string AutoOffsetReset { get; set; } = "latest";
}
