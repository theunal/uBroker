using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using uBroker;
using uBroker.Diagnostics;
using uBroker.Kafka.Serialization;

namespace uBroker.Kafka;

/// <summary>
/// Confluent.Kafka consumer implementing ICheckpointableConsumer.
/// 
/// Kafka mapping:
/// - destination → topic name
/// - ConsumeOptions.ConsumerGroup → consumer group (required)
/// - CheckpointAsync → commits offset for the partition
/// </summary>
public sealed class KafkaConsumer : ICheckpointableConsumer, IAsyncDisposable, IDisposable
{
    private readonly KafkaOptions _options;
    private readonly IMessageSerializer _serializer;
    private readonly UBrokerDiagnostics _diagnostics;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly List<IConsumer<string, byte[]>> _consumers = new();
    private bool _disposed;

    public KafkaConsumer(
        IOptions<KafkaOptions> options,
        IMessageSerializer serializer,
        UBrokerDiagnostics diagnostics,
        ILogger<KafkaConsumer> logger)
    {
        _options = options.Value;
        _serializer = serializer;
        _diagnostics = diagnostics;
        _logger = logger;
    }

    /// <summary>
    /// Subscribe to a Kafka topic. Each call creates a new consumer instance
    /// that runs in a background thread.
    /// </summary>
    public IDisposable Subscribe<T>(string topic, Func<T, MessageContext, ValueTask> handler,
        ConsumeOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var consumerGroup = options?.ConsumerGroup ?? _options.ConsumerGroup;

        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = consumerGroup,
            EnableAutoCommit = _options.EnableAutoCommit,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_options.AutoOffsetReset, true),
        };

        var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);
        _consumers.Add(consumer);

        var cts = new CancellationTokenSource();
        var maxConcurrent = options?.MaxConcurrentCalls ?? Environment.ProcessorCount;

        // Start consumer loop in background.
        var consumeTask = Task.Run(async () =>
        {
            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(cts.Token);
                        if (result is null) continue;

                        var body = result.Message.Value;
                        var headers = new Dictionary<string, string>();
                        if (result.Message.Headers is not null)
                        {
                            foreach (var header in result.Message.Headers)
                            {
                                headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                            }
                        }

                        var context = new MessageContext
                        {
                            Destination = topic,
                            Headers = headers,
                            PartitionOrShardId = result.Partition.ToString(),
                            EnqueuedTime = result.Message.Timestamp.UtcDateTime,
                            Body = body,
                            Topic = result.Topic,
                            Partition = result.Partition.Value,
                            Offset = result.Offset.Value,
                        };

                        // Set checkpoint function for ICheckpointableConsumer.
                        context.CheckpointFn = (ctx, ct) =>
                        {
                            try
                            {
                                consumer.Commit(result);
                                _logger.LogTrace("Committed offset {Offset} for {Topic}/{Partition}",
                                    result.Offset, result.Topic, result.Partition);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Failed to commit offset for {Topic}/{Partition}",
                                    result.Topic, result.Partition);
                            }
                            return ValueTask.CompletedTask;
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
                        _diagnostics.RecordMessagesConsumed(1, serializerTag);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consume error: {Error}", ex.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException) { }
        }, cts.Token);

        return new Subscription(consumer, cts, consumeTask, topic, _logger);
    }

    /// <summary>
    /// Checkpoint consumption progress (commit offsets).
    /// </summary>
    public ValueTask CheckpointAsync(MessageContext context, CancellationToken ct = default)
    {
        return context.CheckpointAsync(ct);
    }

    private sealed class Subscription : IDisposable
    {
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly CancellationTokenSource _cts;
        private readonly Task _consumeTask;
        private readonly string _topic;
        private readonly ILogger _logger;
        private bool _disposed;

        public Subscription(IConsumer<string, byte[]> consumer, CancellationTokenSource cts,
            Task consumeTask, string topic, ILogger logger)
        {
            _consumer = consumer;
            _cts = cts;
            _consumeTask = consumeTask;
            _topic = topic;
            _logger = logger;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _cts.Cancel();
            // Wait for the consume loop to exit before closing the consumer.
            // This prevents the native crash when Consume is still active.
            try { _consumeTask.Wait(TimeSpan.FromSeconds(5)); }
            catch { /* timeout or already completed */ }
            _consumer.Close();
            _consumer.Dispose();
            _cts.Dispose();
            _logger.LogInformation("Unsubscribed from Kafka topic {Topic}", _topic);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        foreach (var consumer in _consumers)
        {
            try { consumer.Close(); consumer.Dispose(); } catch { }
        }
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var consumer in _consumers)
        {
            try { consumer.Close(); consumer.Dispose(); } catch { }
        }
    }
}
