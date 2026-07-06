namespace uBroker;

/// <summary>
/// Broker-agnostic per-subscription consumer options.
/// 
/// Each provider reads only the fields relevant to its protocol:
/// - RabbitMQ: PrefetchCount, MaxConcurrentCalls, DeadLetterExchange, AutoAck
/// - Kafka: ConsumerGroup (required), MaxConcurrentCalls
/// - Azure Service Bus: PrefetchCount, MaxConcurrentCalls, AutoCompleteMessages
/// - Azure Event Hubs: ConsumerGroup (required), MaxConcurrentCalls, BatchSize
/// - AWS SQS: MaxConcurrentCalls, VisibilityTimeoutSeconds, WaitTimeSeconds
/// </summary>
public sealed class ConsumeOptions
{
    /// <summary>
    /// Kafka/Event Hubs: consumer group name (required for these brokers).
    /// RabbitMQ/SQS/Service Bus: ignored.
    /// </summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// RabbitMQ/Service Bus: prefetch count (max unacknowledged messages delivered).
    /// Kafka/SQS/Event Hubs: ignored.
    /// </summary>
    public ushort? PrefetchCount { get; set; }

    /// <summary>
    /// Maximum number of concurrent message processing tasks.
    /// Supported by all providers.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// RabbitMQ: dead-letter exchange for poison messages.
    /// Others: ignored (use broker-side dead-lettering).
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// RabbitMQ/Service Bus: auto-acknowledge messages (fire-and-forget).
    /// WARNING: Disabling manual ack means lost messages on consumer crash.
    /// Kafka: auto-commit (EnableAutoCommit). Manual commit is recommended.
    /// SQS: N/A (visibility timeout-based).
    /// </summary>
    public bool AutoAck { get; set; } = false;

    /// <summary>
    /// Azure Event Hubs: maximum number of events per processing batch.
    /// Others: ignored.
    /// </summary>
    public int? BatchSize { get; set; }

    /// <summary>
    /// AWS SQS: visibility timeout in seconds (how long a message is hidden after receive).
    /// Others: ignored.
    /// </summary>
    public int? VisibilityTimeoutSeconds { get; set; }

    /// <summary>
    /// AWS SQS: long polling wait time in seconds (0 = short polling).
    /// Others: ignored.
    /// </summary>
    public int? WaitTimeSeconds { get; set; }
}
