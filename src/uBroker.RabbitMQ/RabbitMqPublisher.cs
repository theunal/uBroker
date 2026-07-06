using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using uBroker.Diagnostics;
using uBroker.RabbitMQ.Internals;
using uBroker.RabbitMQ.Serialization;
using uBroker.RawBinary;

namespace uBroker.RabbitMQ;

/// <summary>
/// High-performance RabbitMQ publisher with batch support.
/// 
/// Publish flow:
/// 1. Serialize message using Utf8JsonMessageSerializer (zero-allocation).
/// 2. Build BasicProperties with headers (traceparent for distributed tracing).
/// 3. Enqueue into BatchPublishWorker's channel (System.Threading.Channels).
/// 4. BatchPublishWorker drains and publishes via BasicPublishAsync.
/// 
/// RabbitMQ mapping:
/// - destination → exchange name
/// - PublishOptions.RoutingKey → routing key
/// - PublishOptions.Headers → BasicProperties.Headers
/// </summary>
public sealed class RabbitMqPublisher : IUBrokerPublisher, IAsyncDisposable, IDisposable
{
    private readonly BatchPublishWorker _batchWorker;
    private readonly RabbitMqOptions _options;
    private readonly UBrokerDiagnostics _diagnostics;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly Utf8JsonMessageSerializer _jsonSerializer = new();
    private bool _disposed;

    public RabbitMqPublisher(
        BatchPublishWorker batchWorker,
        IOptions<RabbitMqOptions> options,
        UBrokerDiagnostics diagnostics,
        ILogger<RabbitMqPublisher> logger)
    {
        _batchWorker = batchWorker;
        _options = options.Value;
        _diagnostics = diagnostics;
        _logger = logger;
    }

    /// <summary>
    /// Publish a message to the specified exchange.
    /// The destination maps to the RabbitMQ exchange name.
    /// Routing key is taken from PublishOptions.RoutingKey.
    /// </summary>
    public ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var exchange = destination;
        var routingKey = options?.RoutingKey ?? "";

        try
        {
            ReadOnlyMemory<byte> buffer;
            string contentType;
            byte[]? rentedBuffer = null;

            if (typeof(T).IsValueType && RawBinaryTypeInfo<T>.IsEligible)
            {
                var size = UnmanagedBlitSerializer.GetSize<T>();
                var rented = ArrayPool<byte>.Shared.Rent(size);
                UnmanagedBlitSerializer.Write(in message, rented);
                buffer = rented.AsMemory(0, size);
                contentType = WireFormat.RawBinaryContentType;
                rentedBuffer = rented;
            }
            else
            {
                var bufferWriter = new ArrayBufferWriter<byte>(4096);
                var written = _jsonSerializer.Serialize(message, bufferWriter);
                buffer = bufferWriter.WrittenMemory;
                contentType = WireFormat.JsonContentType;
                rentedBuffer = null;
            }

            // Build BasicProperties inline.
            var props = new BasicProperties
            {
                ContentType = contentType,
                ContentEncoding = "utf-8",
                DeliveryMode = options?.DeliveryMode.HasValue == true
                    ? (DeliveryModes)options.DeliveryMode.Value
                    : DeliveryModes.Persistent,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            };

            // Propagate traceparent for distributed tracing.
            var activity = Activity.Current;
            if (activity is not null)
            {
                props.Headers ??= new Dictionary<string, object?>();
                props.Headers["traceparent"] = activity.Id!;
            }

            // Merge per-message headers.
            if (options?.Headers is { Count: > 0 })
            {
                props.Headers ??= new Dictionary<string, object?>();
                foreach (var (key, value) in options.Headers)
                {
                    props.Headers[key] = value!;
                }
            }

            // Set TTL if specified.
            if (options?.TimeToLiveMs is { } ttl)
            {
                props.Expiration = ttl.ToString();
            }

            using var publishActivity = _diagnostics.StartPublishActivity(exchange, routingKey);

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var request = new PublishRequest
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Body = buffer,
                Properties = props,
                Tcs = tcs,
                RentedBuffer = rentedBuffer,
            };

            if (!_batchWorker.Writer.TryWrite(request))
            {
                return WriteSlowAsync(request, ct);
            }

            _logger.LogTrace("Enqueued message to {Exchange}/{RoutingKey}", exchange, routingKey);
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            _diagnostics.RecordPublishError();
            _logger.LogError(ex, "Failed to publish message to {Exchange}/{RoutingKey}", exchange, routingKey);
            return ValueTask.FromException(ex);
        }
    }

    private async ValueTask WriteSlowAsync(PublishRequest request, CancellationToken ct)
    {
        await _batchWorker.Writer.WaitToWriteAsync(ct).ConfigureAwait(false);
        _batchWorker.Writer.TryWrite(request);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }
}
