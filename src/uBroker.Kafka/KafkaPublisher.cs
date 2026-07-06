using System.Buffers;
using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using uBroker.Diagnostics;

namespace uBroker.Kafka;

/// <summary>
/// Confluent.Kafka publisher implementing IPartitionedPublisher.
/// 
/// Kafka mapping:
/// - destination → topic name
/// - PublishOptions.PartitionKey → Message.Key (used by partitioner)
/// - PublishOptions.Headers → Message Headers
/// </summary>
public sealed class KafkaPublisher(
    IProducer<string, byte[]> producer,
    IMessageSerializer serializer,
    UBrokerDiagnostics diagnostics,
    ILogger<KafkaPublisher> logger) : IPartitionedPublisher, IAsyncDisposable, IDisposable
{
    private bool _disposed;

    /// <summary>
    /// Publish a message to a Kafka topic. The destination maps to the topic name.
    /// Partition is determined by the partitioner using the key from PublishOptions.PartitionKey.
    /// </summary>
    public ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        return PublishCoreAsync(destination, options?.PartitionKey, message, options, ct);
    }

    /// <summary>
    /// Publish a message to a specific partition (via partition key).
    /// </summary>
    public ValueTask PublishAsync<T>(string destination, string partitionKey, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        return PublishCoreAsync(destination, partitionKey, message, options, ct);
    }

    private ValueTask PublishCoreAsync<T>(string topic, string? partitionKey, T message,
        PublishOptions? options, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            byte[] body;
            string contentType;

            if (typeof(T).IsValueType && RawBinaryTypeInfo<T>.IsEligible)
            {
                var size = UnmanagedBlitSerializer.GetSize<T>();
                body = new byte[size];
                UnmanagedBlitSerializer.Write(in message, body);
                contentType = WireFormat.RawBinaryContentType;
            }
            else
            {
                var bufferWriter = new ArrayBufferWriter<byte>(4096);
                var written = serializer.Serialize(message, bufferWriter);
                body = bufferWriter.WrittenSpan.Slice(0, written).ToArray();
                contentType = WireFormat.JsonContentType;
            }

            var kafkaMessage = new Message<string, byte[]>
            {
                Key = partitionKey!,
                Value = body,
            };

            // Propagate headers.
            if (options?.Headers is { Count: > 0 })
            {
                kafkaMessage.Headers ??= new Headers();
                foreach (var (key, value) in options.Headers)
                {
                    kafkaMessage.Headers.Add(key, Encoding.UTF8.GetBytes(value?.ToString() ?? ""));
                }
            }

            // Propagate traceparent.
            var activity = Activity.Current;
            if (activity is not null)
            {
                kafkaMessage.Headers ??= new Headers();
                kafkaMessage.Headers.Add("traceparent", Encoding.UTF8.GetBytes(activity.Id!));
            }

            // Set wire format content-type.
            kafkaMessage.Headers ??= new Headers();
            kafkaMessage.Headers.Add(WireFormat.ContentTypeHeaderKey, Encoding.UTF8.GetBytes(contentType));

            using var publishActivity = diagnostics.StartPublishActivity(topic, partitionKey ?? "");

            // ProduceAsync is the synchronous-over-async path. Confluent.Kafka handles
            // internal buffering, so this doesn't block on network I/O.
            producer.Produce(topic, kafkaMessage, report =>
            {
                if (report.Error.IsError)
                {
                    logger.LogError("Kafka publish failed: {Error}", report.Error.Reason);
                }
            });

            diagnostics.RecordMessagesPublished(1, contentType == WireFormat.RawBinaryContentType ? "raw" : "json");
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            diagnostics.RecordPublishError();
            logger.LogError(ex, "Failed to publish to Kafka topic {Topic}", topic);
            return ValueTask.FromException(ex);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        producer.Flush(CancellationToken.None);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        producer.Flush(TimeSpan.FromSeconds(5));
    }
}
