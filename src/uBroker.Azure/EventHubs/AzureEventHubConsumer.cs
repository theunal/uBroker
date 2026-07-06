using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using uBroker.Diagnostics;
using uBroker.Azure.Internals;
using uBroker.RawBinary;

namespace uBroker.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs consumer implementing ICheckpointableConsumer.
/// 
/// Mapping:
/// - destination → event hub name
/// - ConsumeOptions.ConsumerGroup → consumer group (defaults to $Default)
/// - CheckpointAsync → writes blob checkpoint via BlobCheckpointStore
/// </summary>
public sealed class AzureEventHubConsumer(
    IOptions<AzureEventHubOptions> options,
    UBrokerDiagnostics diagnostics,
    ILogger<AzureEventHubConsumer> logger) : ICheckpointableConsumer, IAsyncDisposable, IDisposable
{
    private readonly AzureEventHubOptions _options = options.Value;
    private readonly List<EventProcessorClient> _processors = [];
    private bool _disposed;

    /// <inheritdoc/>
    public IDisposable Subscribe<T>(string eventHubName, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var consumerGroup = options?.ConsumerGroup ?? _options.ConsumerGroup;

        var blobContainerClient = new BlobContainerClient(
            _options.CheckpointStorageConnectionString,
            _options.BlobContainerName);

        var processor = new EventProcessorClient(
            blobContainerClient,
            consumerGroup,
            _options.ConnectionString,
            eventHubName);

        _processors.Add(processor);

        processor.ProcessEventAsync += async args =>
        {
            try
            {
                var body = args.Data.Body.ToArray();
                var headers = new Dictionary<string, string>();
                if (args.Data.Properties is not null)
                {
                    foreach (var (key, value) in args.Data.Properties)
                    {
                        headers[key] = value?.ToString() ?? "";
                    }
                }

                var context = new MessageContext
                {
                    Destination = eventHubName,
                    Headers = headers,
                    PartitionOrShardId = args.Partition.PartitionId,
                    EnqueuedTime = args.Data.EnqueuedTime.UtcDateTime,
                    Body = body,
                    SequenceNumber = args.Data.SequenceNumber,
                };

                // Set checkpoint function.
                context.CheckpointFn = async (ctx, ct) =>
                {
                    await args.UpdateCheckpointAsync(ct).ConfigureAwait(false);
                    logger.LogTrace("Checkpointed Event Hub {EventHub} partition {Partition} at offset {Sequence}",
                        eventHubName, args.Partition.PartitionId, args.Data.SequenceNumber);
                };

                // Deserialize the message.
                T message;
                if (typeof(T) == typeof(byte[]))
                {
                    message = (T)(object)body;
                }
                else if (typeof(T) == typeof(ReadOnlyMemory<byte>))
                {
                    message = (T)(object)new ReadOnlyMemory<byte>(body);
                }
                else if (RawBinaryTypeInfo<T>.IsEligible
                         && headers.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctHeader)
                         && ctHeader == WireFormat.RawBinaryContentType)
                {
                    message = UnmanagedBlitSerializer.Read<T>(body);
                }
                else
                {
                    var serializer = new Utf8JsonMessageSerializer();
                    message = serializer.Deserialize<T>(body);
                }

                await handler(message, context).ConfigureAwait(false);
                var serializerTag = headers.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctHdr)
                    && ctHdr == WireFormat.RawBinaryContentType ? "raw" : "json";
                diagnostics.RecordMessagesConsumed(1, serializerTag);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing Event Hub event");
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            logger.LogError(args.Exception, "Event Hub error: {Operation}", args.Operation);
            return Task.CompletedTask;
        };

        processor.StartProcessingAsync().GetAwaiter().GetResult();

        logger.LogInformation("Subscribed to Event Hub {EventHub} (group: {Group})",
            eventHubName, consumerGroup);

        return new Subscription(processor, eventHubName, consumerGroup, logger);
    }

    /// <inheritdoc/>
    public ValueTask CheckpointAsync(MessageContext context, CancellationToken ct = default)
    {
        return context.CheckpointAsync(ct);
    }

    private sealed class Subscription(EventProcessorClient processor, string eventHub, string group, ILogger logger) : IAsyncDisposable, IDisposable
    {
        private readonly ILogger _logger = logger;
        private bool _disposed;

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            await processor.StopProcessingAsync().ConfigureAwait(false);
            _logger.LogInformation("Unsubscribed from Event Hub {EventHub} (group: {Group})", eventHub, group);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            processor.StopProcessingAsync().GetAwaiter().GetResult();
            _logger.LogInformation("Unsubscribed from Event Hub {EventHub} (group: {Group})", eventHub, group);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        foreach (var processor in _processors)
        {
            try { processor.StopProcessingAsync().GetAwaiter().GetResult(); } catch { }
        }
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var processor in _processors)
        {
            try { processor.StopProcessingAsync().GetAwaiter().GetResult(); } catch { }
        }
    }
}
