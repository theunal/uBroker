namespace uBroker;

/// <summary>
/// Broker-agnostic message publisher interface.
/// 
/// Why a single `destination` parameter instead of broker-specific fields:
/// - RabbitMQ uses exchange + routing key, Kafka uses topic, SQS uses queue URL, etc.
/// - By abstracting to a single `destination` string, user code remains broker-agnostic.
/// - Each provider maps `destination` to its native concept (exchange/topic/queue).
/// - Broker-specific options (partition key, session ID, etc.) go into PublishOptions.
/// 
/// All methods return ValueTask to minimize state machine allocations on the hot path.
/// </summary>
public interface IUBrokerPublisher
{
    /// <summary>
    /// Publish a message to the specified destination.
    /// The destination is mapped to the broker's native concept by each provider:
    /// - RabbitMQ: destination = exchange, routing key in PublishOptions
    /// - Kafka: destination = topic
    /// - Azure Service Bus: destination = queue or topic name
    /// - Azure Event Hubs: destination = event hub name
    /// - AWS SQS: destination = queue URL or name
    /// - AWS SNS: destination = topic ARN or name
    /// </summary>
    /// <typeparam name="T">Message type (must be serializable).</typeparam>
    /// <param name="destination">Target destination (exchange/topic/queue/ARN).</param>
    /// <param name="message">Message payload.</param>
    /// <param name="options">Optional per-message publish options (headers, partition key, etc.).</param>
    /// <param name="ct">Cancellation token.</param>
    ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default);
}
