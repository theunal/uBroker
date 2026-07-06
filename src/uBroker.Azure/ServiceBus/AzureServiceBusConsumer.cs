using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using uBroker.Azure.Internals;
using uBroker.Diagnostics;
using uBroker.RawBinary;

namespace uBroker.Azure.ServiceBus;

/// <summary>
/// Azure Service Bus consumer implementing IUBrokerConsumer.
/// 
/// Mapping:
/// - destination → queue or topic/subscription
/// - ConsumeOptions.PrefetchCount → ProcessorOptions.PrefetchCount
/// - ConsumeOptions.MaxConcurrentCalls → ProcessorOptions.MaxConcurrentCalls
/// </summary>
public sealed class AzureServiceBusConsumer(
    ServiceBusClient client,
    UBrokerDiagnostics diagnostics,
    ILogger<AzureServiceBusConsumer> logger) : IUBrokerConsumer, IAsyncDisposable, IDisposable
{
    private readonly List<ServiceBusProcessor> _processors = [];
    private bool _disposed;

    /// <inheritdoc/>
    public IDisposable Subscribe<T>(string destination, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var processorOptions = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = options?.MaxConcurrentCalls ?? Environment.ProcessorCount,
            PrefetchCount = options?.PrefetchCount ?? 250,
            AutoCompleteMessages = options?.AutoAck ?? false,
        };

        var processor = client.CreateProcessor(destination, processorOptions);
        _processors.Add(processor);

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                var body = args.Message.Body.ToArray();
                var headers = new Dictionary<string, string>();
                foreach (var (key, value) in args.Message.ApplicationProperties)
                {
                    headers[key] = value?.ToString() ?? "";
                }

                var context = new MessageContext
                {
                    Destination = destination,
                    Headers = headers,
                    EnqueuedTime = args.Message.EnqueuedTime,
                    Body = body,
                    SequenceNumber = args.Message.SequenceNumber,
                    SessionId = args.Message.SessionId,
                };

                // Set ack function.
                context.AckFn = async (tag, multiple) =>
                {
                    await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
                };

                // Set nack function.
                context.NackFn = async (tag, multiple, requeue) =>
                {
                    if (requeue)
                        await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
                    else
                        await args.DeadLetterMessageAsync(args.Message).ConfigureAwait(false);
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
                logger.LogError(ex, "Error processing Service Bus message");
                await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            logger.LogError(args.Exception, "Service Bus error: {Source}", args.ErrorSource);
            return Task.CompletedTask;
        };

        processor.StartProcessingAsync().GetAwaiter().GetResult();

        logger.LogInformation("Subscribed to Service Bus {Destination}", destination);

        return new Subscription(processor, destination, logger);
    }

    private sealed class Subscription(ServiceBusProcessor processor, string destination, ILogger logger) : IAsyncDisposable, IDisposable
    {
        private readonly ILogger _logger = logger;
        private bool _disposed;

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            await processor.StopProcessingAsync().ConfigureAwait(false);
            _logger.LogInformation("Unsubscribed from Service Bus {Destination}", destination);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            processor.StopProcessingAsync().GetAwaiter().GetResult();
            _logger.LogInformation("Unsubscribed from Service Bus {Destination}", destination);
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
