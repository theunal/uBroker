using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace uBroker.Diagnostics;

/// <summary>
/// Observability layer for uBroker. Provides metrics via System.Diagnostics.Metrics
/// and distributed tracing via ActivitySource.
///
/// Metrics (Meter-based):
/// - ubroker.publish.messages_per_second: publish throughput counter
/// - ubroker.consume.messages_per_second: consume throughput counter
/// - ubroker.publish.batch_size: histogram of batch sizes (reveals batching efficiency)
/// - ubroker.publish.batch_flush_reason: counter for timeout vs size flushes
/// - ubroker.channel.pool_rent: channel pool rent counter
/// - ubroker.channel.pool_return: channel pool return counter
/// - ubroker.gc.gen0_collections: GC Gen0 collection counter (for baseline comparison)
///
/// Tracing (ActivitySource):
/// - Each publish creates an Activity that flows through the batch worker and into the broker.
/// - The traceparent header is propagated via AMQP headers for distributed tracing.
/// </summary>
public sealed class UBrokerDiagnostics
{
    public const string MeterName = "uBroker";
    public const string ActivitySourceName = "uBroker";

    private readonly Meter _meter;
    private readonly ActivitySource _activitySource;

    // Counters
    private readonly Counter<long> _messagesPublished;
    private readonly Counter<long> _messagesConsumed;
    private readonly Counter<long> _batchFlushByTimeout;
    private readonly Counter<long> _batchFlushBySize;
    private readonly Counter<long> _channelPoolRent;
    private readonly Counter<long> _channelPoolReturn;
    private readonly Counter<long> _publishErrors;

    // Histograms
    private readonly Histogram<int> _batchSizeHistogram;
    private readonly Histogram<double> _publishLatencyHistogram;

    // Up-down counters
    private readonly UpDownCounter<int> _activeChannels;

    public UBrokerDiagnostics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        _activitySource = new ActivitySource(ActivitySourceName, "1.0.0");

        _messagesPublished = _meter.CreateCounter<long>(
            "ubroker.publish.messages",
            description: "Total messages published");

        _messagesConsumed = _meter.CreateCounter<long>(
            "ubroker.consume.messages",
            description: "Total messages consumed");

        _batchFlushByTimeout = _meter.CreateCounter<long>(
            "ubroker.publish.batch.flush.timeout",
            description: "Number of batch flushes triggered by time window expiry");

        _batchFlushBySize = _meter.CreateCounter<long>(
            "ubroker.publish.batch.flush.size",
            description: "Number of batch flushes triggered by batch size limit");

        _batchSizeHistogram = _meter.CreateHistogram<int>(
            "ubroker.publish.batch.size",
            unit: "messages",
            description: "Distribution of batch sizes at flush time");

        _publishLatencyHistogram = _meter.CreateHistogram<double>(
            "ubroker.publish.latency",
            unit: "ms",
            description: "End-to-end publish latency in milliseconds");

        _channelPoolRent = _meter.CreateCounter<long>(
            "ubroker.channel.pool.rent",
            description: "Total channel pool rent operations");

        _channelPoolReturn = _meter.CreateCounter<long>(
            "ubroker.channel.pool.return",
            description: "Total channel pool return operations");

        _publishErrors = _meter.CreateCounter<long>(
            "ubroker.publish.errors",
            description: "Total publish errors (failed deliveries)");

        _activeChannels = _meter.CreateUpDownCounter<int>(
            "ubroker.channel.active",
            description: "Number of currently active (rented) channels");
    }

    /// <summary>Create a new Activity for a publish operation (distributed tracing).</summary>
    public Activity? StartPublishActivity(string exchange, string routingKey)
    {
        return _activitySource.StartActivity(
            name: $"uBroker.Publish {exchange} {routingKey}",
            kind: ActivityKind.Producer,
            tags:
            [
                new("messaging.system", "rabbitmq"),
                new("messaging.destination.name", exchange),
                new("messaging.rabbitmq.routing_key", routingKey),
            ]);
    }

    /// <summary>Create a new Activity for a consume operation.</summary>
    public Activity? StartConsumeActivity(string queue, string? exchange = null)
    {
        return _activitySource.StartActivity(
            name: $"uBroker.Consume {queue}",
            kind: ActivityKind.Consumer,
            tags:
            [
                new("messaging.system", "rabbitmq"),
                new("messaging.destination.name", queue),
            ]);
    }

    public void RecordMessagesPublished(int count, string serializer = "json") =>
        _messagesPublished.Add(count, new KeyValuePair<string, object?>("serializer", serializer));

    public void RecordBatchPublished(int batchSize)
    {
        _batchSizeHistogram.Record(batchSize);
        if (batchSize > 1)
            _batchFlushBySize.Add(1);
    }

    public void RecordMessagesConsumed(int count, string serializer = "json") =>
        _messagesConsumed.Add(count, new KeyValuePair<string, object?>("serializer", serializer));

    public void RecordBatchFlushReason(string reason)
    {
        if (reason == "timeout")
            _batchFlushByTimeout.Add(1);
        else
            _batchFlushBySize.Add(1);
    }

    public void RecordBatchSize(int size) =>
        _batchSizeHistogram.Record(size);

    public void RecordPublishLatency(double milliseconds) =>
        _publishLatencyHistogram.Record(milliseconds);

    public void RecordChannelRented()
    {
        _channelPoolRent.Add(1);
        _activeChannels.Add(1);
    }

    public void RecordChannelReturned()
    {
        _channelPoolReturn.Add(1);
        _activeChannels.Add(-1);
    }

    public void RecordPublishError() =>
        _publishErrors.Add(1);

    /// <summary>Get the ActivitySource for registering with OpenTelemetry.</summary>
    public ActivitySource GetActivitySource() => _activitySource;

    /// <summary>Get the Meter for registering with OpenTelemetry.</summary>
    public Meter GetMeter() => _meter;
}
