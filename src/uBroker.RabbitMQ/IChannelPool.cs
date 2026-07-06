namespace uBroker.RabbitMQ;

/// <summary>
/// Abstracts IChannel pooling for RabbitMQ.
/// 
/// Why this is in uBroker.RabbitMQ, not Core:
/// - Core is broker-agnostic — channel pooling is a RabbitMQ-specific concept.
/// - Kafka/Azure/AWS clients handle their own connection pooling internally.
/// - Only RabbitMQ needs explicit channel pooling (IChannel is not thread-safe).
/// </summary>
public interface IChannelPool : IDisposable
{
    /// <summary>
    /// Rent a channel from the pool. Dispose the returned PooledChannel to return it.
    /// </summary>
    ValueTask<PooledChannel> RentAsync(CancellationToken ct = default);
}

/// <summary>
/// A rented channel wrapper. Dispose to return to pool.
/// </summary>
public sealed class PooledChannel(object channel, Func<PooledChannel, ValueTask> returnFn) : IAsyncDisposable, IDisposable
{
    private bool _disposed;

    /// <summary>The underlying IChannel.</summary>
    public object Channel { get; } = channel;

    /// <summary>Whether this channel is still valid (open, not faulted).</summary>
    public bool IsHealthy { get; set; } = true;

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            return returnFn(this);
        }
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            returnFn(this).AsTask().GetAwaiter().GetResult();
        }
    }
}
