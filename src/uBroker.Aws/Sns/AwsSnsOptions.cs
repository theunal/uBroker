using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;

namespace uBroker.Aws.Sns;

/// <summary>
/// AWS SNS configuration options.
/// 
/// SNS is a pub/sub service — it does NOT support consuming messages directly.
/// Typical usage: SNS → SQS fan-out. The SQS consumer handles message receipt.
/// </summary>
public sealed class AwsSnsOptions
{
    /// <summary>AWS region (e.g. "eu-central-1").</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>AWS access key (optional, uses SDK default credentials chain if null).</summary>
    public string? AccessKey { get; set; }

    /// <summary>AWS secret key (optional, uses SDK default credentials chain if null).</summary>
    public string? SecretKey { get; set; }

    /// <summary>Custom service URL for local emulators (e.g. LocalStack at "http://localhost:4566"). Null uses the default AWS endpoint.</summary>
    public string? ServiceURL { get; set; }

    /// <summary>Creates an AmazonSimpleNotificationServiceClient from these options.</summary>
    internal Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient CreateClient()
    {
        var config = new AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(Region),
        };

        if (ServiceURL is not null)
        {
            config.ServiceURL = ServiceURL;
        }

        if (AccessKey is not null && SecretKey is not null)
        {
            return new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient(
                new BasicAWSCredentials(AccessKey, SecretKey), config);
        }

        return new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient(config);
    }
}
