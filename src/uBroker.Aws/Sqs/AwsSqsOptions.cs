using Amazon;
using Amazon.Runtime;
using Amazon.SQS;

namespace uBroker.Aws.Sqs;

/// <summary>
/// AWS SQS configuration options.
/// </summary>
public sealed class AwsSqsOptions
{
    /// <summary>AWS region (e.g. "eu-central-1").</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>AWS access key (optional, uses SDK default credentials chain if null).</summary>
    public string? AccessKey { get; set; }

    /// <summary>AWS secret key (optional, uses SDK default credentials chain if null).</summary>
    public string? SecretKey { get; set; }

    /// <summary>Default visibility timeout in seconds.</summary>
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 30;

    /// <summary>Custom service URL for local emulators (e.g. LocalStack at "http://localhost:4566"). Null uses the default AWS endpoint.</summary>
    public string? ServiceURL { get; set; }

    /// <summary>Maximum messages per batch (AWS SQS limit: 10).</summary>
    public int MaxBatchSize { get; set; } = 10;

    /// <summary>Long polling wait time in seconds (0 = short polling).</summary>
    public int WaitTimeSeconds { get; set; } = 2;

    /// <summary>Creates an AmazonSQSClient from these options.</summary>
    internal Amazon.SQS.AmazonSQSClient CreateClient()
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(Region),
        };

        if (ServiceURL is not null)
        {
            config.ServiceURL = ServiceURL;
        }

        if (AccessKey is not null && SecretKey is not null)
        {
            return new Amazon.SQS.AmazonSQSClient(
                new BasicAWSCredentials(AccessKey, SecretKey), config);
        }

        return new Amazon.SQS.AmazonSQSClient(config);
    }
}
