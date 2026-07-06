namespace uBroker;

/// <summary>
/// Capability interface for brokers that support checkpoint-based consumption.
/// 
/// Why a separate interface:
/// - Queue-based brokers (RabbitMQ, SQS) use ack/nack per message.
/// - Log-based brokers (Kafka, Event Hubs) use offset/checkpoint per batch.
/// - Forcing both models into one interface creates confusion and API pollution.
/// - ICheckpointableConsumer extends IUBrokerConsumer, so queue-based consumers
///   use the base interface, while log-based consumers use checkpoint semantics.
/// 
/// Supported by: Kafka (offset commit), Azure Event Hubs (blob checkpoint).
/// Not supported by: RabbitMQ, Azure Service Bus (use IUBrokerConsumer with ack/nack).
/// </summary>
public interface ICheckpointableConsumer : IUBrokerConsumer
{
    /// <summary>
    /// Checkpoint the consumption progress up to the given context.
    /// For Kafka: commits the offset for the partition.
    /// For Event Hubs: writes the blob checkpoint for the partition.
    /// 
    /// Call this after successfully processing a batch of messages.
    /// Idempotent: calling multiple times with the same context is safe.
    /// </summary>
    /// <param name="context">The message context to checkpoint to.</param>
    /// <param name="ct">Cancellation token.</param>
    ValueTask CheckpointAsync(MessageContext context, CancellationToken ct = default);
}
