using System.Buffers;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using uBroker.Diagnostics;

namespace uBroker.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs publisher implementing IPartitionedPublisher.
/// 
/// Mapping:
/// - destination → event hub name
/// - PublishOptions.PartitionKey → EventData.PartitionKey
/// - PublishOptions.Headers → EventData.Properties
/// </summary>
public sealed class AzureEventHubPublisher(
    EventHubProducerClient producer,
    IOptions<AzureEventHubOptions> options,
    IMessageSerializer serializer,
    UBrokerDiagnostics diagnostics,
    ILogger<AzureEventHubPublisher> logger) : IPartitionedPublisher, IAsyncDisposable, IDisposable
{
    private readonly AzureEventHubOptions _options = options.Value;
    private bool _disposed;

    /// <inheritdoc/>
    public ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        return PublishCoreAsync(destination, options?.PartitionKey, message, options, ct);
    }

    /// <inheritdoc/>
    public ValueTask PublishAsync<T>(string destination, string partitionKey, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        return PublishCoreAsync(destination, partitionKey, message, options, ct);
    }

    private async ValueTask PublishCoreAsync<T>(string eventHubName, string? partitionKey,
        T message, PublishOptions? options, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            EventData eventData;

            if (typeof(T).IsValueType && RawBinaryTypeInfo<T>.IsEligible)
            {
                var size = UnmanagedBlitSerializer.GetSize<T>();
                var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
                try
                {
                    UnmanagedBlitSerializer.Write(in message, rented);
                    eventData = new EventData(rented.AsMemory(0, size));
                    eventData.Properties[WireFormat.ContentTypeHeaderKey] = WireFormat.RawBinaryContentType;
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(rented);
                }
            }
            else
            {
                var bufferWriter = new ArrayBufferWriter<byte>(4096);
                var written = serializer.Serialize(message, bufferWriter);
                eventData = new EventData(bufferWriter.WrittenMemory.Slice(0, written));
                eventData.Properties[WireFormat.ContentTypeHeaderKey] = WireFormat.JsonContentType;
            }

            // Propagate headers.
            if (options?.Headers is { Count: > 0 })
            {
                foreach (var (key, value) in options.Headers)
                {
                    eventData.Properties[key] = value!;
                }
            }

            // Propagate traceparent.
            var activity = System.Diagnostics.Activity.Current;
            if (activity is not null)
            {
                eventData.Properties["traceparent"] = activity.Id!;
            }

            using var publishActivity = diagnostics.StartPublishActivity(eventHubName, partitionKey ?? "");

            // Create a batch and send.
            using var batch = await producer.CreateBatchAsync(new CreateBatchOptions
            {
                PartitionKey = partitionKey,
            }, ct).ConfigureAwait(false);

            batch.TryAdd(eventData);
            await producer.SendAsync(batch, ct).ConfigureAwait(false);

            var serializerTag = eventData.Properties.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctVal)
                && ctVal is string s && s == WireFormat.RawBinaryContentType ? "raw" : "json";
            diagnostics.RecordMessagesPublished(1, serializerTag);
            logger.LogTrace("Published to Event Hub {EventHub}", eventHubName);
        }
        catch (Exception ex)
        {
            diagnostics.RecordPublishError();
            logger.LogError(ex, "Failed to publish to Event Hub {EventHub}", eventHubName);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await producer.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
