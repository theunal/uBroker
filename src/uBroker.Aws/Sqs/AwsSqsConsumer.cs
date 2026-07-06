using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using uBroker.Aws.Serialization;
using uBroker.Diagnostics;
using uBroker.RawBinary;

namespace uBroker.Aws.Sqs;

/// <summary>
/// AWS SQS consumer implementing IUBrokerConsumer.
/// 
/// Mapping:
/// - destination → queue URL or queue name
/// - ConsumeOptions.VisibilityTimeoutSeconds → VisibilityTimeout
/// - ConsumeOptions.WaitTimeSeconds → WaitTimeSeconds (long polling)
/// - ConsumeOptions.MaxConcurrentCalls → number of parallel pollers
/// 
/// Ack = DeleteMessageAsync(ReceiptHandle)
/// Nack (requeue) = ChangeMessageVisibility(0)
/// </summary>
public sealed class AwsSqsConsumer(
    AmazonSQSClient client,
    IOptions<AwsSqsOptions> options,
    UBrokerDiagnostics diagnostics,
    ILogger<AwsSqsConsumer> logger) : IUBrokerConsumer, IAsyncDisposable, IDisposable
{
    private readonly AwsSqsOptions _options = options.Value;
    private readonly List<Task> _pollers = [];
    private readonly List<CancellationTokenSource> _ctsList = [];
    private bool _disposed;

    /// <inheritdoc/>
    public IDisposable Subscribe<T>(string destination, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var visibilityTimeout = options?.VisibilityTimeoutSeconds ?? _options.DefaultVisibilityTimeoutSeconds;
        var waitTime = options?.WaitTimeSeconds ?? _options.WaitTimeSeconds;
        var maxConcurrent = options?.MaxConcurrentCalls ?? Environment.ProcessorCount;

        var cts = new CancellationTokenSource();
        _ctsList.Add(cts);

        // Start multiple pollers for concurrent processing.
        var pollerTasks = new List<Task>();
        for (var i = 0; i < maxConcurrent; i++)
        {
            var pollerCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
            _ctsList.Add(pollerCts);

            var pollerTask = Task.Run(async () =>
            {
                while (!pollerCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var receiveRequest = new ReceiveMessageRequest
                        {
                            QueueUrl = destination,
                            MaxNumberOfMessages = _options.MaxBatchSize,
                            WaitTimeSeconds = waitTime,
                            VisibilityTimeout = visibilityTimeout,
                            MessageAttributeNames = ["All"],
                        };

                        var response = await client.ReceiveMessageAsync(receiveRequest, pollerCts.Token)
                            .ConfigureAwait(false);

                        foreach (var sqsMessage in response.Messages)
                        {
                            try
                            {
                                var headers = new Dictionary<string, string>();
                                foreach (var (key, attr) in sqsMessage.MessageAttributes)
                                {
                                    headers[key] = attr.StringValue ?? "";
                                }

                                var context = new MessageContext
                                {
                                    Destination = destination,
                                    Headers = headers,
                                    EnqueuedTime = sqsMessage.Attributes.TryGetValue("SentTimestamp", out var sentTs)
                                        ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(sentTs))
                                        : DateTimeOffset.UtcNow,
                                    Body = Encoding.UTF8.GetBytes(sqsMessage.Body),
                                    ReceiptHandle = sqsMessage.ReceiptHandle,
                                };

                                // Ack = delete message from queue.
                                context.AckFn = async (tag, multiple) =>
                                {
                                    await client.DeleteMessageAsync(destination, sqsMessage.ReceiptHandle, pollerCts.Token)
                                        .ConfigureAwait(false);
                                };

                                // Nack (requeue) = set visibility to 0.
                                context.NackFn = async (tag, multiple, requeue) =>
                                {
                                    if (requeue)
                                    {
                                        await client.ChangeMessageVisibilityAsync(
                                            destination, sqsMessage.ReceiptHandle, 0, pollerCts.Token)
                                            .ConfigureAwait(false);
                                    }
                                };

                                // Deserialize the message.
                                T message;
                                var bodyBytes = Encoding.UTF8.GetBytes(sqsMessage.Body);
                                if (typeof(T) == typeof(byte[]))
                                {
                                    message = (T)(object)bodyBytes;
                                }
                                else if (typeof(T) == typeof(ReadOnlyMemory<byte>))
                                {
                                    message = (T)(object)new ReadOnlyMemory<byte>(bodyBytes);
                                }
                                else if (RawBinaryTypeInfo<T>.IsEligible
                                         && headers.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctHeader)
                                         && ctHeader == WireFormat.RawBinaryContentType)
                                {
                                    message = UnmanagedBlitSerializer.Read<T>(bodyBytes);
                                }
                                else
                                {
                                    var serializer = new Utf8JsonMessageSerializer();
                                    message = serializer.Deserialize<T>(bodyBytes);
                                }

                                await handler(message, context).ConfigureAwait(false);
                                var serializerTag = headers.TryGetValue(WireFormat.ContentTypeHeaderKey, out var ctHdr)
                                    && ctHdr == WireFormat.RawBinaryContentType ? "raw" : "json";
                                diagnostics.RecordMessagesConsumed(1, serializerTag);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Error processing SQS message {MessageId}", sqsMessage.MessageId);
                            }
                        }
                    }
                    catch (OperationCanceledException) when (pollerCts.Token.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "SQS poll error");
                        await Task.Delay(1000, pollerCts.Token).ConfigureAwait(false);
                    }
                }
            }, pollerCts.Token);

            pollerTasks.Add(pollerTask);
        }

        _pollers.AddRange(pollerTasks);

        logger.LogInformation("Subscribed to SQS {Destination} ({Concurrency} pollers)",
            destination, maxConcurrent);

        return new Subscription(destination, cts, logger);
    }

    private sealed class Subscription(string destination, CancellationTokenSource cts, ILogger logger) : IDisposable
    {
        private readonly ILogger _logger = logger;
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            cts.Cancel();
            cts.Dispose();
            _logger.LogInformation("Unsubscribed from SQS {Destination}", destination);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var cts in _ctsList)
        {
            try { cts.Cancel(); cts.Dispose(); } catch { }
        }
        // Wait for poller tasks to exit gracefully.
        if (_pollers.Count > 0)
        {
            try { await Task.WhenAll(_pollers).WaitAsync(TimeSpan.FromSeconds(5)); }
            catch { /* timeout or already completed */ }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var cts in _ctsList)
        {
            try { cts.Cancel(); cts.Dispose(); } catch { }
        }
    }
}
