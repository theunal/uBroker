using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;

namespace uBroker.RabbitMQ.Internals;

/// <summary>
/// Object pool policy for RabbitMQ IChannel instances.
///
/// Why this design:
/// - IChannel is NOT thread-safe in RabbitMQ.Client 7.x: each concurrent publisher/consumer needs its own channel.
/// - Creating channels is expensive (~1-2ms per channel due to AMQP channel.open negotiation).
/// - Pooling channels amortizes this cost across millions of messages.
/// - Health checking on Return ensures we never hand out a dead channel to the next caller.
/// - In 7.x, publisher confirms are handled transparently — no ConfirmSelect needed.
/// </summary>
internal sealed class ChannelPooledObjectPolicy(IConnection connection, ushort prefetchCount) : PooledObjectPolicy<IChannel>
{

    /// <summary>
    /// Create a new channel. Called when the pool is empty or a dirty channel is detected.
    /// In RabbitMQ.Client 7.x, CreateChannelAsync is the async equivalent of CreateModel.
    /// Publisher confirms are handled transparently — no ConfirmSelectAsync needed.
    /// </summary>
    public override IChannel Create()
    {
        // Synchronous create for ObjectPool compatibility.
        // This is acceptable because channel creation is infrequent (pool warmup / recovery).
        var channel = connection.CreateChannelAsync()
            .GetAwaiter().GetResult();

        // BasicQos sets prefetch per channel, crucial for fair dispatch under load.
        // In 7.x, publisher confirms are automatic — no ConfirmSelect needed.
        channel.BasicQosAsync(prefetchSize: 0, prefetchCount: prefetchCount, global: false)
            .GetAwaiter().GetResult();

        return channel;
    }

    /// <summary>
    /// Validate a returned channel. Returns true if healthy, false if it should be discarded.
    /// Called when a channel is returned to the pool — dirty channels are replaced, not reused.
    /// </summary>
    public override bool Return(IChannel channel)
    {
        // IsOpen is the cheapest health check. A faulted/closed channel cannot be reused.
        // The overhead of checking is negligible (~nanoseconds) vs creating a new channel (~1-2ms).
        if (channel.IsOpen)
        {
            return true;
        }

        // Channel is dead — dispose it and signal the pool to discard.
        try { channel.Dispose(); } catch { /* best effort cleanup */ }
        return false;
    }
}
