using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;

namespace uBroker.RabbitMQ.Internals;

/// <summary>
/// Manages a pool of IChannel (RabbitMQ channel) instances.
///
/// Architecture:
/// - Uses ObjectPool&lt;IChannel&gt; backed by ChannelPooledObjectPolicy for thread-safe pooling.
/// - Each rent/return pair is wrapped in PooledChannel for RAII-style cleanup.
/// - Connection is shared (single connection strategy): AMQP connections are heavyweight,
///   and RabbitMQ handles multiplexing channels over a single TCP connection efficiently.
///
/// Thread safety:
/// - ObjectPool&lt;T&gt; is thread-safe by default (DefaultObjectPool uses ConcurrentBag internally).
/// - IChannel is NOT thread-safe, but each caller gets their own instance from the pool.
/// - Return validates channel health before returning to pool, preventing leaked dead channels.
/// </summary>
internal sealed class ChannelManager(ObjectPool<IChannel> pool, ILogger<ChannelManager> logger) : IChannelPool
{
    private bool _disposed;

    /// <summary>
    /// Rent a channel from the pool. The returned PooledChannel wraps the IChannel
    /// and handles automatic return-on-dispose.
    /// </summary>
    public ValueTask<PooledChannel> RentAsync(CancellationToken ct = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ChannelManager));

        var channel = pool.Get();

        logger.LogDebug("Rented channel #{ChannelId}", channel.ChannelNumber);

        var pooled = new PooledChannel(channel, ReturnAsync);
        return ValueTask.FromResult(pooled);
    }

    /// <summary>
    /// Return a channel to the pool. If the channel is healthy (IsOpen), it goes back
    /// for reuse. If unhealthy, it's disposed and the pool creates a fresh one on next rent.
    /// </summary>
    private ValueTask ReturnAsync(PooledChannel pooled)
    {
        if (_disposed)
        {
            try { ((IChannel)pooled.Channel).Dispose(); } catch { /* best effort */ }
            return ValueTask.CompletedTask;
        }

        if (pooled.IsHealthy)
        {
            pool.Return((IChannel)pooled.Channel);
            logger.LogDebug("Returned channel #{ChannelId} to pool",
                ((IChannel)pooled.Channel).ChannelNumber);
        }
        else
        {
            try { ((IChannel)pooled.Channel).Dispose(); } catch { /* best effort */ }
            logger.LogWarning("Discarded unhealthy channel (pool will create new on next rent)");
        }

        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        logger.LogInformation("ChannelManager disposed");
    }
}
