namespace uBroker;

/// <summary>
/// Broker-agnostic per-publish options.
/// 
/// Why all fields are optional/nullable:
/// - Each provider only reads the fields relevant to its protocol.
/// - RabbitMQ reads: RoutingKey, TimeToLiveMs, DeliveryMode, Headers
/// - Kafka reads: PartitionKey, Headers
/// - Azure Service Bus reads: SessionId, Headers
/// - Azure Event Hubs reads: PartitionKey, Headers
/// - AWS SQS reads: MessageGroupId, DeduplicationId, Headers, TimeToLiveSeconds
/// - AWS SNS reads: Headers, MessageGroupId (for FIFO topics)
/// 
/// Users set only what their target broker needs; unknown fields are silently ignored.
/// </summary>
public sealed class PublishOptions
{
    /// <summary>
    /// RabbitMQ: routing key for the message.
    /// Kafka/Event Hubs: ignored (use PartitionKey instead).
    /// Others: ignored.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Kafka/Event Hubs: key used to determine which partition receives the message.
    /// RabbitMQ/SQS/SNS/Service Bus: ignored.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Azure Service Bus: session ID for session-enabled queues/topics.
    /// Others: ignored.
    /// </summary>
    public string? SessionId { get; set; }

    /// <summary>
    /// AWS SQS FIFO: message group ID for ordering within a group.
    /// AWS SNS FIFO: message group ID for ordering within a FIFO topic.
    /// Others: ignored.
    /// </summary>
    public string? MessageGroupId { get; set; }

    /// <summary>
    /// AWS SQS FIFO: deduplication ID for idempotent publishing (5-minute window).
    /// Others: ignored.
    /// </summary>
    public string? DeduplicationId { get; set; }

    /// <summary>
    /// RabbitMQ: message TTL in milliseconds. Null means no TTL.
    /// AWS SQS: message TTL in seconds (via MessageAttributes).
    /// Others: ignored.
    /// </summary>
    public int? TimeToLiveMs { get; set; }

    /// <summary>
    /// RabbitMQ: delivery mode (1=transient, 2=persistent).
    /// Others: ignored.
    /// </summary>
    public byte? DeliveryMode { get; set; }

    /// <summary>
    /// Message headers/properties. Each provider maps these to its native header mechanism:
    /// - RabbitMQ: BasicProperties.Headers
    /// - Kafka: MessageHeader
    /// - Service Bus: ApplicationProperties
    /// - Event Hubs: EventData.Properties
    /// - SQS: MessageAttributes (string values only)
    /// - SNS: MessageAttributes (string values only)
    /// </summary>
    public IDictionary<string, object>? Headers { get; set; }
}
