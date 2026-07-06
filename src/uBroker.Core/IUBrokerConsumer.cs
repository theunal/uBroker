namespace uBroker;

/// <summary>
/// Broker-agnostic message consumer interface.
/// 
/// Why `destination` instead of broker-specific fields:
/// - Same rationale as IUBrokerPublisher: abstract the native concept away.
/// - RabbitMQ: destination = queue name
/// - Kafka: destination = topic (consumer group in ConsumeOptions)
/// - Azure Service Bus: destination = queue or topic/subscription
/// - AWS SQS: destination = queue URL or name
/// 
/// Subscribe returns IDisposable for graceful unsubscribe (RAII pattern).
/// </summary>
public interface IUBrokerConsumer
{
    /// <summary>
    /// Subscribe to messages on the specified destination.
    /// The handler receives deserialized messages and a MessageContext for ack/nack/checkpoint.
    /// </summary>
    /// <typeparam name="T">Message type to deserialize.</typeparam>
    /// <param name="destination">Source destination (queue/topic).</param>
    /// <param name="handler">Async message handler (ValueTask for zero allocation).</param>
    /// <param name="options">Optional per-subscription consumer options.</param>
    /// <returns>IDisposable — dispose to unsubscribe and clean up resources.</returns>
    IDisposable Subscribe<T>(string destination, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null);
}
