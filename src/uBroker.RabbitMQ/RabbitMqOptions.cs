namespace uBroker.RabbitMQ;

/// <summary>
/// RabbitMQ-specific configuration options.
/// 
/// Why separate from Core: Core is broker-agnostic and has no AMQP concepts.
/// Each provider owns its options class, registered via its own DI extension.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>AMQP connection string (e.g. amqp://guest:guest@localhost:5672).</summary>
    public string ConnectionString { get; set; } = default!;

    /// <summary>Maximum messages to batch before flushing to the broker.</summary>
    public int BatchMaxSize { get; set; } = 500;

    /// <summary>Maximum time window to wait before flushing a partial batch.</summary>
    public TimeSpan BatchMaxWindow { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>
    /// Number of IChannel instances to pool.
    /// IChannel is NOT thread-safe, so each concurrent publisher needs its own.
    /// </summary>
    public int ChannelPoolSize { get; set; } = Environment.ProcessorCount * 2;

    /// <summary>RabbitMQ prefetch count.</summary>
    public ushort PrefetchCount { get; set; } = 250;

    /// <summary>Maximum concurrent message processing tasks per consumer.</summary>
    public int Concurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>Enable automatic connection/channel recovery.</summary>
    public bool EnableAutomaticRecovery { get; set; } = true;

    /// <summary>Dead-letter exchange for poison messages. Null disables dead-lettering.</summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>Maximum retry attempts for publish operations.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Base delay for exponential backoff on retries.</summary>
    public TimeSpan BaseRetryDelay { get; set; } = TimeSpan.FromMilliseconds(10);

    /// <summary>Maximum delay between retries (exponential backoff ceiling).</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(2);
}
