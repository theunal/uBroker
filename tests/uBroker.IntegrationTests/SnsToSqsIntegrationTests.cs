using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using uBroker;
using uBroker.Aws.Sns;
using uBroker.Aws.Sqs;
using uBroker.Aws.Serialization;
using uBroker.Diagnostics;
using Xunit;

namespace uBroker.IntegrationTests;

/// <summary>
/// SNS→SQS fan-out integration tests.
/// Verifies that a message published to SNS arrives in a subscribed SQS queue.
/// </summary>
public class SnsToSqsIntegrationTests : IAsyncLifetime
{
    private AmazonSQSClient? _sqsClient;
    private AmazonSimpleNotificationServiceClient? _snsClient;
    private AwsSnsPublisher? _snsPublisher;

    private static string ServiceUrl =>
        Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";

    private static string Region =>
        Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-central-1";

    public ValueTask InitializeAsync()
    {
        var creds = new Amazon.Runtime.BasicAWSCredentials("test", "test");

        var sqsConfig = new AmazonSQSConfig
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(Region),
            ServiceURL = ServiceUrl,
        };
        _sqsClient = new AmazonSQSClient(creds, sqsConfig);

        var snsConfig = new AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(Region),
            ServiceURL = ServiceUrl,
        };
        _snsClient = new AmazonSimpleNotificationServiceClient(creds, snsConfig);

        var diag = new UBrokerDiagnostics();
        _snsPublisher = new AwsSnsPublisher(
            _snsClient,
            new Utf8JsonMessageSerializer(),
            diag,
            NullLogger<AwsSnsPublisher>.Instance);

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _snsPublisher?.Dispose();
        _snsClient?.Dispose();
        _sqsClient?.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PublishToSns_MessageArrivesInSubscribedSqsQueue()
    {
        // 1. Create SQS queue
        var queueName = $"ubroker-sns-sqs-{Guid.NewGuid():N}";
        var createQueueResp = await _sqsClient!.CreateQueueAsync(queueName);
        var queueUrl = createQueueResp.QueueUrl;

        // Get queue ARN
        var queueAttrResp = await _sqsClient.GetQueueAttributesAsync(queueUrl,
            ["QueueArn"]);
        var queueArn = queueAttrResp.Attributes["QueueArn"];

        // 2. Create SNS topic
        var topicResp = await _snsClient!.CreateTopicAsync($"ubroker-test-{Guid.NewGuid():N}");
        var topicArn = topicResp.TopicArn;

        // 3. Subscribe SQS queue to SNS topic
        await _snsClient.SubscribeAsync(topicArn, "sqs", queueArn);

        // Allow subscription propagation in LocalStack
        await Task.Delay(1000);

        // 4. Publish via uBroker SNS publisher
        var message = new SnsTestMessage { Id = 99, Content = "sns-to-sqs" };
        await _snsPublisher!.PublishAsync(topicArn, message);

        // 5. Poll SQS to verify message arrived
        var received = false;
        for (int i = 0; i < 10; i++)
        {
            var receiveResp = await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 2,
                MessageAttributeNames = ["All"],
            });

            if (receiveResp.Messages.Count > 0)
            {
                received = true;
                var body = receiveResp.Messages[0].Body;
                Assert.Contains("sns-to-sqs", body);
                break;
            }
        }

        Assert.True(received, "Message did not arrive in SQS queue within timeout");

        // Cleanup
        var subs = await _snsClient.ListSubscriptionsByTopicAsync(topicArn);
        foreach (var sub in subs.Subscriptions)
            await _snsClient.UnsubscribeAsync(sub.SubscriptionArn);
        await _snsClient.DeleteTopicAsync(topicArn);
        await _sqsClient.DeleteQueueAsync(queueUrl);
    }

    [Fact]
    public async Task PublishToSns_WithHeaders_PropagatedToSqs()
    {
        var queueName = $"ubroker-sns-headers-{Guid.NewGuid():N}";
        var createQueueResp = await _sqsClient!.CreateQueueAsync(queueName);
        var queueUrl = createQueueResp.QueueUrl;

        var queueAttrResp = await _sqsClient.GetQueueAttributesAsync(queueUrl, ["QueueArn"]);
        var queueArn = queueAttrResp.Attributes["QueueArn"];

        var topicResp = await _snsClient!.CreateTopicAsync($"ubroker-hdr-{Guid.NewGuid():N}");
        var topicArn = topicResp.TopicArn;

        await _snsClient.SubscribeAsync(topicArn, "sqs", queueArn);
        await Task.Delay(1000);

        await _snsPublisher!.PublishAsync(topicArn, new SnsTestMessage { Id = 1, Content = "with-headers" },
            new PublishOptions { Headers = new Dictionary<string, object> { ["custom-key"] = "custom-value" } });

        var received = false;
        for (int i = 0; i < 10; i++)
        {
            var receiveResp = await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 2,
                MessageAttributeNames = ["All"],
            });

            if (receiveResp.Messages.Count > 0)
            {
                received = true;
                var body = receiveResp.Messages[0].Body;
                Assert.Contains("with-headers", body);
                break;
            }
        }

        Assert.True(received, "Message with headers did not arrive in SQS queue");

        var subs2 = await _snsClient.ListSubscriptionsByTopicAsync(topicArn);
        foreach (var sub in subs2.Subscriptions)
            await _snsClient.UnsubscribeAsync(sub.SubscriptionArn);
        await _snsClient.DeleteTopicAsync(topicArn);
        await _sqsClient.DeleteQueueAsync(queueUrl);
    }
}

public class SnsTestMessage
{
    public int Id { get; set; }
    public string Content { get; set; } = default!;
}
