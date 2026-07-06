using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using uBroker.Diagnostics;
using uBroker.RabbitMQ.Serialization;

namespace uBroker.RabbitMQ;

/// <summary>
/// High-performance RabbitMQ consumer with channel pooling and concurrency control.
///
/// Consume flow:
/// 1. Declare queue (durable, with optional dead-letter exchange).
/// 2. Create a dedicated channel per subscription (from pool).
/// 3. Set prefetch count for fair dispatch.
/// 4. Register async event handler that deserializes and dispatches to handlers.
/// 5. Each handler runs in a SemaphoreSlim-bounded concurrent pipeline.
///
/// In RabbitMQ.Client 7.x:
/// - Publisher confirms are handled transparently (no ConfirmSelect needed).
/// - AsyncEventingBasicConsumer uses ReceivedAsync event.
/// - All methods are async (BasicConsumeAsync, BasicAckAsync, etc.).
/// </summary>
public sealed class RabbitMqConsumer(
    IConnection connection,
    IOptions<RabbitMqOptions> options,
    UBrokerDiagnostics diagnostics,
    ILogger<RabbitMqConsumer> logger) : IUBrokerConsumer, IAsyncDisposable, IDisposable
{
    private readonly RabbitMqOptions _options = options.Value;
    private readonly ConcurrentBag<IDisposable> _subscriptions = [];
    private bool _disposed;

    /// <summary>
    /// Subscribe to messages on the specified queue.
    /// Returns IDisposable — dispose to unsubscribe and release the channel.
    /// </summary>
    public IDisposable Subscribe<T>(string queue, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var prefetchCount = options?.PrefetchCount ?? _options.PrefetchCount;
        var concurrency = options?.MaxConcurrentCalls > 0 ? options.MaxConcurrentCalls : _options.Concurrency;

        // Create a dedicated channel for this subscription.
        var channel = connection.CreateChannelAsync()
            .GetAwaiter().GetResult();

        // In 7.x, publisher confirms are automatic — no ConfirmSelect needed.
        channel.BasicQosAsync(prefetchSize: 0, prefetchCount: prefetchCount, global: false)
            .GetAwaiter().GetResult();

        // Declare the queue with optional dead-letter exchange.
        var deadLetterExchange = options?.DeadLetterExchange ?? _options.DeadLetterExchange;
        var args = new Dictionary<string, object?>();

        if (!string.IsNullOrEmpty(deadLetterExchange))
        {
            args["x-dead-letter-exchange"] = deadLetterExchange;
        }

        channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: args.Count > 0 ? args : null)
            .GetAwaiter().GetResult();

        // SemaphoreSlim controls concurrency — prevents overwhelming downstream services.
        var semaphore = new SemaphoreSlim(concurrency, concurrency);
        var serializer = new Utf8JsonMessageSerializer();

        // Register the consumer.
        // RabbitMQ.Client 7.x: AsyncEventingBasicConsumer uses ReceivedAsync event.
        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, ea) =>
        {
            await semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                // IMPORTANT: In RabbitMQ.Client 7.x, the body memory is only valid
                // within the event handler scope. We must copy it if we need it later.
                var body = ea.Body.ToArray();
                var memory = new ReadOnlyMemory<byte>(body);

                var activity = diagnostics.StartConsumeActivity(queue, ea.Exchange);

                try
                {
                    // Build MessageContext for the handler.
                    var context = new MessageContext
                    {
                        Destination = queue,
                        DeliveryTag = ea.DeliveryTag,
                        Redelivered = ea.Redelivered,
                        Exchange = ea.Exchange,
                        RoutingKey = ea.RoutingKey,
                        Headers = ea.BasicProperties?.Headers is not null
                            ? new Dictionary<string, string>(
                                ea.BasicProperties.Headers
                                    .Where(kv => kv.Value is not null)
                                    .Select(kv => new KeyValuePair<string, string>(kv.Key, kv.Value!.ToString() ?? "")))
                            : [],
                        Body = memory,
                        EnqueuedTime = ea.BasicProperties?.Timestamp.UnixTime is { } ts
                            ? DateTimeOffset.FromUnixTimeSeconds(ts)
                            : DateTimeOffset.UtcNow,
                        // Set up ack/nack function via the channel.
                        AckFn = (tag, multiple) =>
                            {
                                try
                                {
                                    // RabbitMQ.Client 7.x: AckAsync with CancellationToken.
                                    channel.BasicAckAsync(deliveryTag: tag, multiple: multiple, cancellationToken: default)
                                        .GetAwaiter().GetResult();
                                    return ValueTask.CompletedTask;
                                }
                                catch (Exception ex)
                                {
                                    logger.LogError(ex, "Failed to ack message {DeliveryTag}", tag);
                                    return ValueTask.CompletedTask;
                                }
                            }
                    };

                    // Deserialize the message.
                    T? message;
                    if (typeof(T) == typeof(byte[]))
                    {
                        message = (T)(object)body;
                    }
                    else if (typeof(T) == typeof(ReadOnlyMemory<byte>) || typeof(T) == typeof(ReadOnlySequence<byte>))
                    {
                        message = (T)(object)memory;
                    }
                    else if (RawBinaryTypeInfo<T>.IsEligible
                             && ea.BasicProperties?.ContentType == WireFormat.RawBinaryContentType)
                    {
                        message = UnmanagedBlitSerializer.Read<T>(body);
                    }
                    else
                    {
                        message = serializer.Deserialize<T>(body);
                    }

                    // Invoke the handler.
                    await handler(message!, context).ConfigureAwait(false);
                    var serializerTag = ea.BasicProperties?.ContentType == WireFormat.RawBinaryContentType ? "raw" : "json";
                    diagnostics.RecordMessagesConsumed(1, serializerTag);

                    logger.LogTrace("Consumed message from {Queue} (delivery={DeliveryTag})",
                        queue, ea.DeliveryTag);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing message from {Queue} (delivery={DeliveryTag})",
                        queue, ea.DeliveryTag);

                    // Nack with requeue for transient errors.
                    try
                    {
                        await channel.BasicNackAsync(
                            deliveryTag: ea.DeliveryTag,
                            multiple: false,
                            requeue: true,
                            cancellationToken: default).ConfigureAwait(false);
                    }
                    catch (Exception nackEx)
                    {
                        logger.LogError(nackEx, "Failed to nack message {DeliveryTag}", ea.DeliveryTag);
                    }
                }
                finally
                {
                    activity?.Dispose();
                }
            }
            finally
            {
                semaphore.Release();
            }
        };

        // Auto-ack mode: ack immediately if configured.
        var autoAck = options?.AutoAck ?? false;
        var consumerTag = channel.BasicConsumeAsync(
            queue: queue,
            autoAck: autoAck,
            consumer: consumer)
            .GetAwaiter().GetResult();

        logger.LogInformation(
            "Subscribed to queue {Queue} (prefetch={Prefetch}, concurrency={Concurrency}, autoAck={AutoAck})",
            queue, prefetchCount, concurrency, autoAck);

        // Return a subscription handle that cleans up the channel on dispose.
        return new Subscription(channel, consumerTag, queue, logger);
    }

    /// <summary>Internal subscription handle that cleans up the channel on dispose.</summary>
    private sealed class Subscription(IChannel channel, string consumerTag, string queue, ILogger logger) : IDisposable
    {
        private readonly ILogger _logger = logger;
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                channel.BasicCancelAsync(consumerTag).GetAwaiter().GetResult();
                channel.CloseAsync().GetAwaiter().GetResult();
                channel.Dispose();
                _logger.LogInformation("Unsubscribed from queue {Queue}", queue);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cleaning up subscription for queue {Queue}", queue);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        foreach (var subscription in _subscriptions)
        {
            try { subscription.Dispose(); } catch { /* best effort cleanup */ }
        }

        logger.LogInformation("RabbitMqConsumer disposed ({Count} subscriptions cleaned up)",
            _subscriptions.Count);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var subscription in _subscriptions)
        {
            try { subscription.Dispose(); } catch { /* best effort cleanup */ }
        }

        logger.LogInformation("RabbitMqConsumer disposed");
    }
}
