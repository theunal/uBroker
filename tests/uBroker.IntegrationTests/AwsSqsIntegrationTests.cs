using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using uBroker;
using uBroker.Aws.Sqs;
using uBroker.Aws.Serialization;
using uBroker.Diagnostics;
using Xunit;

namespace uBroker.IntegrationTests;

public class AwsSqsIntegrationTests : IAsyncLifetime
{
    private AmazonSQSClient? _sqsClient;
    private AwsSqsPublisher? _publisher;
    private AwsSqsConsumer? _consumer;

    private static string ServiceUrl =>
        Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";

    private static string Region =>
        Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-central-1";

    public ValueTask InitializeAsync()
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(Region),
            ServiceURL = ServiceUrl,
        };
        _sqsClient = new AmazonSQSClient(new Amazon.Runtime.BasicAWSCredentials("test", "test"), config);
        var options = new AwsSqsOptions { Region = Region, ServiceURL = ServiceUrl, AccessKey = "test", SecretKey = "test", WaitTimeSeconds = 1 };
        var diag = new UBrokerDiagnostics();
        _publisher = new AwsSqsPublisher(_sqsClient, new Utf8JsonMessageSerializer(), diag, NullLogger<AwsSqsPublisher>.Instance);
        _consumer = new AwsSqsConsumer(_sqsClient, Options.Create(options), diag, NullLogger<AwsSqsConsumer>.Instance);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _consumer?.Dispose();
        _publisher?.Dispose();
        _sqsClient?.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<string> CreateQueueAndGetUrl(string name)
    {
        var resp = await _sqsClient!.CreateQueueAsync(name);
        return resp.QueueUrl;
    }

    [Fact]
    public async Task PublishAsync_ShouldNotThrow()
    {
        var url = await CreateQueueAndGetUrl($"test-{Guid.NewGuid():N}");
        await _publisher!.PublishAsync(url, new SqsTestMessage { Id = 1, Content = "hello" });
    }

    [Fact]
    public async Task PublishAndConsume_ShouldReceiveMessage()
    {
        var url = await CreateQueueAndGetUrl($"test-{Guid.NewGuid():N}");
        var message = new SqsTestMessage { Id = 42, Content = "sqs-integration" };
        await _publisher!.PublishAsync(url, message);

        // Poll directly with SQS client to verify round-trip.
        var response = await _sqsClient!.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 5,
            MessageAttributeNames = ["All"],
        });

        Assert.Single(response.Messages);
        var body = response.Messages[0].Body;
        Assert.Contains("sqs-integration", body);
    }

    [Fact]
    public async Task PublishWithHeaders_ShouldWork()
    {
        var url = await CreateQueueAndGetUrl($"test-{Guid.NewGuid():N}");
        await _publisher!.PublishAsync(url, new SqsTestMessage { Id = 1, Content = "with-headers" },
            new PublishOptions { Headers = new Dictionary<string, object> { ["source"] = "test" } });

        var response = await _sqsClient!.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url, MaxNumberOfMessages = 1, WaitTimeSeconds = 2, MessageAttributeNames = ["All"],
        });

        Assert.Single(response.Messages);
        Assert.True(response.Messages[0].MessageAttributes.ContainsKey("source"));
    }
}

public class SqsTestMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
}
