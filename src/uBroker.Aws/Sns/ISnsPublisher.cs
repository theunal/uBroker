namespace uBroker.Aws.Sns;

/// <summary>
/// Marker interface for AWS SNS publishers.
/// 
/// Why a separate interface:
/// - SNS is a pub/sub fan-out service, NOT a queue.
/// - SNS does NOT support consuming messages — it publishes to topics,
///   and SQS queues subscribe to those topics.
/// - ISnsPublisher extends IUBrokerPublisher but clearly signals that
///   this is a publish-only provider.
/// - Users who need SNS publish + SQS consume should inject both
///   ISnsPublisher and IUBrokerConsumer (the SQS one).
/// </summary>
public interface ISnsPublisher : IUBrokerPublisher { }
