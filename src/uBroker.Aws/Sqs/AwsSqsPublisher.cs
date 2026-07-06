using System.Buffers;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using uBroker.Diagnostics;
using uBroker.RawBinary;

namespace uBroker.Aws.Sqs;

/// <summary>
/// AWS SQS publisher implementing IUBrokerPublisher.
/// 
/// Mapping:
/// - destination → queue URL or queue name
/// - PublishOptions.MessageGroupId → FIFO queue message group
/// - PublishOptions.DeduplicationId → FIFO queue deduplication
/// - PublishOptions.Headers → MessageAttributes (string values only)
/// 
/// Batch support: Uses SendMessageBatchAsync (max 10 messages per batch per AWS limit).
/// </summary>
public sealed class AwsSqsPublisher(
    AmazonSQSClient client,
    IOptions<AwsSqsOptions> options,
    IMessageSerializer serializer,
    UBrokerDiagnostics diagnostics,
    ILogger<AwsSqsPublisher> logger) : IUBrokerPublisher, IAsyncDisposable, IDisposable
{
    private readonly AwsSqsOptions _options = options.Value;
    private bool _disposed;

    /// <inheritdoc/>
    public async ValueTask PublishAsync<T>(string destination, T message,
        PublishOptions? options = null, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            SendMessageRequest request;

            if (typeof(T).IsValueType && RawBinaryTypeInfo<T>.IsEligible)
            {
                var size = UnmanagedBlitSerializer.GetSize<T>();
                var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
                try
                {
                    UnmanagedBlitSerializer.Write(in message, rented);
                    request = new SendMessageRequest
                    {
                        QueueUrl = destination,
                        MessageBody = Convert.ToBase64String(rented, 0, size),
                        MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
                    };
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(rented);
                }
                request.MessageAttributes[WireFormat.ContentTypeHeaderKey] = new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = WireFormat.RawBinaryContentType,
                };
            }
            else
            {
                var bufferWriter = new ArrayBufferWriter<byte>(4096);
                var written = serializer.Serialize(message, bufferWriter);
                var body = Encoding.UTF8.GetString(bufferWriter.WrittenSpan[..written]);
                request = new SendMessageRequest
                {
                    QueueUrl = destination,
                    MessageBody = body,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>(),
                };
                request.MessageAttributes[WireFormat.ContentTypeHeaderKey] = new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = WireFormat.JsonContentType,
                };
            }

            // Set FIFO queue fields.
            if (options?.MessageGroupId is not null)
            {
                request.MessageGroupId = options.MessageGroupId;
            }
            if (options?.DeduplicationId is not null)
            {
                request.MessageDeduplicationId = options.DeduplicationId;
            }

            // Set message attributes (headers).
            if (options?.Headers is { Count: > 0 })
            {
                foreach (var (key, value) in options.Headers)
                {
                    request.MessageAttributes[key] = new MessageAttributeValue
                    {
                        DataType = "String",
                        StringValue = value?.ToString() ?? "",
                    };
                }
            }

            // Propagate traceparent.
            var activity = System.Diagnostics.Activity.Current;
            if (activity is not null)
            {
                request.MessageAttributes["traceparent"] = new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = activity.Id!,
                };
            }

            await client.SendMessageAsync(request, ct).ConfigureAwait(false);
            var serializerTag = request.MessageAttributes.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctAttr)
                && ctAttr.StringValue == WireFormat.RawBinaryContentType ? "raw" : "json";
            diagnostics.RecordMessagesPublished(1, serializerTag);
            logger.LogTrace("Published to SQS {Destination}", destination);
        }
        catch (Exception ex)
        {
            diagnostics.RecordPublishError();
            logger.LogError(ex, "Failed to publish to SQS {Destination}", destination);
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        client.Dispose();
    }
}
