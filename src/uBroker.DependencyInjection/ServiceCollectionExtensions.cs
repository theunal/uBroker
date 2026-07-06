using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using uBroker.Diagnostics;
using uBroker.RabbitMQ;
using uBroker.RabbitMQ.Internals;
using uBroker.RabbitMQ.Serialization;
using AzureSbClient = Azure.Messaging.ServiceBus.ServiceBusClient;
using AzureSbSender = Azure.Messaging.ServiceBus.ServiceBusSender;
using AzureEhProducer = Azure.Messaging.EventHubs.Producer.EventHubProducerClient;
using AwsSqsClient = Amazon.SQS.AmazonSQSClient;
using AwsSnsClient = Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient;

namespace uBroker.DependencyInjection;

/// <summary>
/// DI registration extensions for uBroker.
/// Each provider has its own AddUBrokerXxx method for granular registration.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Register RabbitMQ provider.</summary>
    public static IServiceCollection AddUBrokerRabbitMQ(
        this IServiceCollection services,
        Action<RabbitMqOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();

        services.TryAddSingleton<IConnection>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>().Value;
            var factory = ConnectionFactoryBuilder.Create(options);
            return ConnectionFactoryBuilder.CreateConnectionWithRetryAsync(factory)
                .GetAwaiter().GetResult();
        });

        services.TryAddSingleton<ObjectPool<IChannel>>(sp =>
        {
            var connection = sp.GetRequiredService<IConnection>();
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>().Value;
            var policy = new ChannelPooledObjectPolicy(connection, options.PrefetchCount);
            return new DefaultObjectPool<IChannel>(policy, options.ChannelPoolSize);
        });

        services.TryAddSingleton<IChannelPool, ChannelManager>();
        services.TryAddSingleton<BatchPublishWorker>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<BatchPublishWorker>());

        services.TryAddSingleton<IUBrokerPublisher, RabbitMqPublisher>();
        services.TryAddSingleton<IUBrokerConsumer, RabbitMqConsumer>();

        return services;
    }

    /// <summary>Register Kafka provider.</summary>
    public static IServiceCollection AddUBrokerKafka(
        this IServiceCollection services,
        Action<uBroker.Kafka.KafkaOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();
        services.TryAddSingleton<IMessageSerializer, Utf8JsonMessageSerializer>();

        services.TryAddSingleton<Confluent.Kafka.IProducer<string, byte[]>>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<uBroker.Kafka.KafkaOptions>>().Value;
            var config = new Confluent.Kafka.ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                LingerMs = options.LingerMs,
                BatchSize = options.BatchSize,
                CompressionType = Enum.Parse<Confluent.Kafka.CompressionType>(options.CompressionType, true),
                Acks = Enum.Parse<Confluent.Kafka.Acks>(options.Acks, true),
            };
            return new Confluent.Kafka.ProducerBuilder<string, byte[]>(config).Build();
        });

        services.TryAddSingleton<IUBrokerPublisher, uBroker.Kafka.KafkaPublisher>();
        services.TryAddSingleton<ICheckpointableConsumer, uBroker.Kafka.KafkaConsumer>();
        services.TryAddSingleton<IPartitionedPublisher>(sp =>
            (IPartitionedPublisher)sp.GetRequiredService<IUBrokerPublisher>());

        return services;
    }

    /// <summary>Register Azure Service Bus provider.</summary>
    public static IServiceCollection AddUBrokerAzureServiceBus(
        this IServiceCollection services,
        Action<uBroker.Azure.ServiceBus.AzureServiceBusOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();
        services.TryAddSingleton<IMessageSerializer, uBroker.Azure.Internals.Utf8JsonMessageSerializer>();

        services.TryAddSingleton<AzureSbClient>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<uBroker.Azure.ServiceBus.AzureServiceBusOptions>>().Value;
            return new AzureSbClient(options.ConnectionString);
        });

        services.TryAddSingleton<AzureSbSender>(sp =>
        {
            var client = sp.GetRequiredService<AzureSbClient>();
            return client.CreateSender("default");
        });

        services.TryAddSingleton<IUBrokerPublisher, uBroker.Azure.ServiceBus.AzureServiceBusPublisher>();
        services.TryAddSingleton<IUBrokerConsumer, uBroker.Azure.ServiceBus.AzureServiceBusConsumer>();

        return services;
    }

    /// <summary>Register Azure Event Hubs provider.</summary>
    public static IServiceCollection AddUBrokerAzureEventHubs(
        this IServiceCollection services,
        Action<uBroker.Azure.EventHubs.AzureEventHubOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();
        services.TryAddSingleton<IMessageSerializer, uBroker.Azure.Internals.Utf8JsonMessageSerializer>();

        services.TryAddSingleton<AzureEhProducer>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<uBroker.Azure.EventHubs.AzureEventHubOptions>>().Value;
            return new AzureEhProducer(options.ConnectionString, "default");
        });

        services.TryAddSingleton<IUBrokerPublisher, uBroker.Azure.EventHubs.AzureEventHubPublisher>();
        services.TryAddSingleton<ICheckpointableConsumer, uBroker.Azure.EventHubs.AzureEventHubConsumer>();
        services.TryAddSingleton<IPartitionedPublisher>(sp =>
            (IPartitionedPublisher)sp.GetRequiredService<IUBrokerPublisher>());

        return services;
    }

    /// <summary>Register AWS SQS provider.</summary>
    public static IServiceCollection AddUBrokerAwsSqs(
        this IServiceCollection services,
        Action<uBroker.Aws.Sqs.AwsSqsOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();
        services.TryAddSingleton<IMessageSerializer, Utf8JsonMessageSerializer>();

        services.TryAddSingleton<AwsSqsClient>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<uBroker.Aws.Sqs.AwsSqsOptions>>().Value;
            return options.CreateClient();
        });

        services.TryAddSingleton<IUBrokerPublisher, uBroker.Aws.Sqs.AwsSqsPublisher>();
        services.TryAddSingleton<IUBrokerConsumer, uBroker.Aws.Sqs.AwsSqsConsumer>();

        return services;
    }

    /// <summary>Register AWS SNS provider (publish-only, no consumer).</summary>
    public static IServiceCollection AddUBrokerAwsSns(
        this IServiceCollection services,
        Action<uBroker.Aws.Sns.AwsSnsOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton<UBrokerDiagnostics>();
        services.TryAddSingleton<IMessageSerializer, Utf8JsonMessageSerializer>();

        services.TryAddSingleton<AwsSnsClient>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<uBroker.Aws.Sns.AwsSnsOptions>>().Value;
            return options.CreateClient();
        });

        services.TryAddSingleton<uBroker.Aws.Sns.ISnsPublisher, uBroker.Aws.Sns.AwsSnsPublisher>();

        return services;
    }
}
