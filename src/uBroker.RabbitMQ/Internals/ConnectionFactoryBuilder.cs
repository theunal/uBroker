using RabbitMQ.Client;

namespace uBroker.RabbitMQ.Internals;

/// <summary>
/// Builds a RabbitMQ ConnectionFactory from RabbitMqOptions.
/// Enforces single-connection strategy and configures automatic recovery.
/// </summary>
internal static class ConnectionFactoryBuilder
{
    /// <summary>
    /// Creates a ConnectionFactory configured for high-throughput, low-latency operation.
    /// Key design decisions:
    /// - Single connection per application lifetime: AMQP connections are heavyweight (TCP + TLS handshake).
    ///   Multiple connections waste broker resources and add latency.
    /// - Automatic recovery enabled: handles broker restarts without application restart.
    /// - Endpoint resolution from connection string: supports amqp://user:pass@host:port/vhost format.
    /// </summary>
    public static ConnectionFactory Create(RabbitMqOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString);

        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = options.EnableAutomaticRecovery,
            TopologyRecoveryEnabled = options.EnableAutomaticRecovery,
            // Net frames should be large enough for batch payloads but not so large they waste memory.
            RequestedFrameMax = 131072,
            // Heartbeat prevents idle connection timeout. 60s is standard for production.
            RequestedHeartbeat = TimeSpan.FromSeconds(60),
            // Use async consumer dispatch to avoid blocking the RabbitMQ reader thread.
            ConsumerDispatchConcurrency = (ushort)options.Concurrency,
            // Parse the AMQP URI from the connection string.
            Uri = new Uri(options.ConnectionString)
        };

        return factory;
    }

    /// <summary>
    /// Creates a connection with retry logic for initial connection establishment.
    /// This handles broker startup delays (e.g. in Docker/k8s environments).
    /// </summary>
    public static async Task<IConnection> CreateConnectionWithRetryAsync(
        ConnectionFactory factory,
        int maxRetries = 5,
        TimeSpan? initialDelay = null,
        CancellationToken ct = default)
    {
        var delay = initialDelay ?? TimeSpan.FromSeconds(1);
        Exception? lastException = null;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastException = ex;

                if (attempt < maxRetries)
                {
                    // Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    delay *= 2;
                }
            }
        }

        throw new InvalidOperationException(
            $"Failed to connect to RabbitMQ after {maxRetries + 1} attempts.", lastException);
    }
}
