namespace uBroker.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs configuration options.
/// </summary>
public sealed class AzureEventHubOptions
{
    /// <summary>Event Hubs namespace connection string (with Manage claim).</summary>
    public string ConnectionString { get; set; } = default!;

    /// <summary>Azure Blob Storage connection string for checkpoint store.</summary>
    public string CheckpointStorageConnectionString { get; set; } = default!;

    /// <summary>Blob container name for checkpoints.</summary>
    public string BlobContainerName { get; set; } = "checkpoints";

    /// <summary>Default consumer group.</summary>
    public string ConsumerGroup { get; set; } = "$Default";

    /// <summary>Maximum batch size for EventHubProducerClient.</summary>
    public int MaxBatchSize { get; set; } = 256;

    /// <summary>Maximum wait time for batching.</summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>Maximum concurrent processing tasks per partition.</summary>
    public int MaxConcurrentCalls { get; set; } = Environment.ProcessorCount;
}
