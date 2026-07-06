namespace uBroker;

/// <summary>
/// Broker-agnostic message consumption context.
/// 
/// Why readonly record struct:
/// - Allocated on the stack when possible (no heap allocation for the struct itself).
/// - Immutable: handlers cannot accidentally mutate context state.
/// - Equality by value: useful for checkpoint comparison in log-based brokers.
/// 
/// Provider-specific fields are mapped as follows:
/// - RabbitMQ: DeliveryTag (ack), Redelivered, Exchange, RoutingKey, Queue
/// - Kafka: Topic, Partition, Offset (used for offset commit)
/// - Service Bus: SequenceNumber, SessionId (used for session lock)
/// - Event Hubs: PartitionKey, SequenceNumber, EventBody
/// - SQS: ReceiptHandle (ack), MessageId
/// - SNS: MessageId (no consumer-side ack in SNS→SQS fan-out)
/// 
/// The Ack/Nack/Checkpoint functions are set by the provider implementation
/// and are not exposed as public properties to keep the API clean.
/// </summary>
public sealed class MessageContext
{
    /// <summary>Destination (queue/topic) this message was consumed from.</summary>
    public string Destination { get; set; } = default!;

    /// <summary>Message headers/properties from the broker.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// Partition or shard identifier (Kafka partition, Event Hub partition, etc.).
    /// Null for queue-based brokers (RabbitMQ, SQS, Service Bus).
    /// </summary>
    public string? PartitionOrShardId { get; set; }

    /// <summary>UTC timestamp when the message was enqueued by the broker.</summary>
    public DateTimeOffset EnqueuedTime { get; set; }

    /// <summary>The raw message body as a read-only byte sequence.</summary>
    public ReadOnlyMemory<byte> Body { get; set; }

    // ── RabbitMQ-specific (set by RabbitMqConsumer) ──
    
    /// <summary>RabbitMQ: delivery tag for manual ack/nack.</summary>
    public ulong DeliveryTag { get; set; }

    /// <summary>RabbitMQ: whether this message was redelivered.</summary>
    public bool Redelivered { get; set; }

    /// <summary>RabbitMQ: exchange the message was published to.</summary>
    public string? Exchange { get; set; }

    /// <summary>RabbitMQ: routing key used.</summary>
    public string? RoutingKey { get; set; }

    // ── Kafka-specific (set by KafkaConsumer) ──

    /// <summary>Kafka: topic name.</summary>
    public string? Topic { get; set; }

    /// <summary>Kafka: partition index.</summary>
    public int? Partition { get; set; }

    /// <summary>Kafka: message offset within the partition.</summary>
    public long? Offset { get; set; }

    // ── Azure Service Bus-specific (set by AzureServiceBusConsumer) ──

    /// <summary>Service Bus: sequence number for checkpointing.</summary>
    public long? SequenceNumber { get; set; }

    /// <summary>Service Bus: session ID (for session-enabled entities).</summary>
    public string? SessionId { get; set; }

    // ── AWS SQS-specific (set by AwsSqsConsumer) ──

    /// <summary>SQS: receipt handle for ack/delete.</summary>
    public string? ReceiptHandle { get; set; }

    // ── Internal: provider-specific ack/nack/checkpoint functions ──

    /// <summary>Internal ack function set by the provider. Queue-based brokers only.</summary>
    internal Func<ulong, bool, ValueTask>? AckFn { get; set; }

    /// <summary>Internal nack function set by the provider. Queue-based brokers only.</summary>
    internal Func<ulong, bool, bool, ValueTask>? NackFn { get; set; }

    /// <summary>Internal checkpoint function set by the provider. Log-based brokers only.</summary>
    internal Func<MessageContext, CancellationToken, ValueTask>? CheckpointFn { get; set; }

    /// <summary>Acknowledge this message (queue-based brokers: RabbitMQ, Service Bus, SQS).</summary>
    public ValueTask AckAsync(bool multiple = false) =>
        AckFn?.Invoke(DeliveryTag, multiple) ?? ValueTask.CompletedTask;

    /// <summary>Nack this message (requeue or discard). Queue-based brokers only.</summary>
    public ValueTask NackAsync(bool requeue = true, bool multiple = false) =>
        NackFn?.Invoke(DeliveryTag, multiple, requeue) ?? ValueTask.CompletedTask;

    /// <summary>Checkpoint consumption progress (log-based brokers: Kafka, Event Hubs).</summary>
    public ValueTask CheckpointAsync(CancellationToken ct = default) =>
        CheckpointFn?.Invoke(this, ct) ?? ValueTask.CompletedTask;
}
