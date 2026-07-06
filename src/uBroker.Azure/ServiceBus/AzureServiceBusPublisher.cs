using System.Buffers;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using uBroker.Diagnostics;

namespace uBroker.Azure.ServiceBus;

/// <summary>
/// Azure Service Bus publisher implementing IUBrokerPublisher.
/// 
/// Mapping:
/// - destination → queue or topic name
/// - PublishOptions.SessionId → ServiceBusMessage.SessionId
/// - PublishOptions.Headers → ServiceBusMessage.ApplicationProperties
/// </summary>
public sealed class AzureServiceBusPublisher(
    ServiceBusSender sender,
    IMessageSerializer serializer,
    UBrokerDiagnostics diagnostics,
    ILogger<AzureServiceBusPublisher> logger) : IUBrokerPublisher, IAsyncDisposable, IDisposable
{
    private bool _disposed;

    /// <inheritdoc/>
    public ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            ServiceBusMessage sbMessage;

            if (typeof(T).IsValueType && RawBinaryTypeInfo<T>.IsEligible)
            {
                var size = UnmanagedBlitSerializer.GetSize<T>();
                var body = new byte[size];
                UnmanagedBlitSerializer.Write(in message, body);
                sbMessage = new ServiceBusMessage(body)
                {
                    ContentType = WireFormat.RawBinaryContentType,
                };
            }
            else
            {
                var bufferWriter = new ArrayBufferWriter<byte>(4096);
                var written = serializer.Serialize(message, bufferWriter);
                sbMessage = new ServiceBusMessage(bufferWriter.WrittenSpan.Slice(0, written).ToArray())
                {
                    ContentType = WireFormat.JsonContentType,
                };
            }

            // Set session ID if specified.
            if (options?.SessionId is not null)
            {
                sbMessage.SessionId = options.SessionId;
            }

            // Set TTL if specified.
            if (options?.TimeToLiveMs is { } ttl)
            {
                sbMessage.TimeToLive = TimeSpan.FromMilliseconds(ttl);
            }

            // Propagate headers.
            if (options?.Headers is { Count: > 0 })
            {
                foreach (var (key, value) in options.Headers)
                {
                    sbMessage.ApplicationProperties[key] = value!;
                }
            }

            // Propagate traceparent.
            var activity = System.Diagnostics.Activity.Current;
            if (activity is not null)
            {
                sbMessage.ApplicationProperties["traceparent"] = activity.Id!;
            }

            using var publishActivity = diagnostics.StartPublishActivity(destination, "");

            // Use SendMessagesAsync for batch support in Service Bus 7.x.
            sender.SendMessageAsync(sbMessage, ct).GetAwaiter().GetResult();

            diagnostics.RecordMessagesPublished(1, sbMessage.ContentType == WireFormat.RawBinaryContentType ? "raw" : "json");
            logger.LogTrace("Published to Service Bus {Destination}", destination);
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            diagnostics.RecordPublishError();
            logger.LogError(ex, "Failed to publish to Service Bus {Destination}", destination);
            return ValueTask.FromException(ex);
        }
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
