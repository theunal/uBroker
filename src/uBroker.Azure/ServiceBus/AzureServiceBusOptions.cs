namespace uBroker.Azure.ServiceBus;

/// <summary>
/// Azure Service Bus configuration options.
/// </summary>
public sealed class AzureServiceBusOptions
{
    /// <summary>Service Bus connection string.</summary>
    public string ConnectionString { get; set; } = default!;

    /// <summary>Maximum concurrent message processing calls.</summary>
    public int MaxConcurrentCalls { get; set; } = Environment.ProcessorCount;

    /// <summary>Prefetch count for the processor.</summary>
    public int PrefetchCount { get; set; } = 250;

    /// <summary>Auto-complete messages after processing.</summary>
    public bool AutoCompleteMessages { get; set; } = false;
}
