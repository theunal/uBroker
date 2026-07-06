namespace uBroker;

/// <summary>
/// Wire format constants used to distinguish raw binary from JSON payloads
/// on the broker. Providers set content-type headers on publish; consumers
/// check them on receive.
///
/// Primary mechanism: provider-native content-type header (ContentType for
/// RabbitMQ, ContentType for Service Bus, content-type property for Event Hubs,
/// message attribute for SQS/SNS, Kafka header).
///
/// Fallback: if a provider doesn't support content-type headers, consumers
/// assume JSON (the legacy default).
/// </summary>
public static class WireFormat
{
    /// <summary>Content-type value for raw binary blitted structs.</summary>
    public const string RawBinaryContentType = "application/x-ubroker-raw";

    /// <summary>Content-type value for JSON-serialized messages.</summary>
    public const string JsonContentType = "application/json";

    /// <summary>Header key used across all providers.</summary>
    public const string ContentTypeHeaderKey = "content-type";
}
