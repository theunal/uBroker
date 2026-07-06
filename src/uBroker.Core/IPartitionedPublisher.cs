namespace uBroker;

/// <summary>
/// Capability interface for brokers that support partitioned publishing.
/// 
/// Why a separate interface instead of adding PartitionKey to IUBrokerPublisher:
/// - Not all brokers support partitioning (RabbitMQ, SQS don't have partitions).
/// - Adding partition key to the base interface would force all providers to implement it
///   or throw NotSupportedException — both are poor API design.
/// - IPartitionedPublisher extends IUBrokerPublisher, so users can still use the base
///   interface for broker-agnostic code, and cast/inject IPartitionedPublisher when they
///   need partition-aware publishing (Kafka, Event Hubs).
/// 
/// Supported by: Kafka (topic partitions), Azure Event Hubs (event hub partitions).
/// Not supported by: RabbitMQ, Azure Service Bus, AWS SQS/SNS.
/// </summary>
public interface IPartitionedPublisher : IUBrokerPublisher
{
    /// <summary>
    /// Publish a message to a specific partition within the destination.
    /// The partition key determines which partition receives the message:
    /// - Kafka: key is used by the partitioner to select partition
    /// - Event Hubs: key is hashed to select partition
    /// </summary>
    /// <typeparam name="T">Message type (must be serializable).</typeparam>
    /// <param name="destination">Target destination (topic/event hub name).</param>
    /// <param name="partitionKey">Key used to determine the target partition.</param>
    /// <param name="message">Message payload.</param>
    /// <param name="options">Optional per-message publish options.</param>
    /// <param name="ct">Cancellation token.</param>
    ValueTask PublishAsync<T>(string destination, string partitionKey, T message,
        PublishOptions? options = null, CancellationToken ct = default);
}
