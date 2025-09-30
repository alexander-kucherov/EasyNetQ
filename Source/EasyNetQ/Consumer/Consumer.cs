using EasyNetQ.Events;
using EasyNetQ.Topology;
using EasyNetQ.Internals;
using EasyNetQ.Persistent;
using Microsoft.Extensions.Logging;

namespace EasyNetQ.Consumer;

/// <summary>
///     Represent an abstract consumer
/// </summary>
public interface IConsumer : IAsyncDisposable
{
    /// <summary>
    ///     Unique consumer id
    /// </summary>
    Guid Id { get; }

    /// <summary>
    ///     Starts the consumer asynchronously
    /// </summary>
    /// <returns>Disposable to stop the consumer</returns>
    Task StartConsumingAsync(CancellationToken cancellationToken = default);
}

/// <summary>
///     Configuration of the consumer for a queue
/// </summary>
public class PerQueueConsumerConfiguration
{
    /// <summary>
    ///     Creates PerQueueConsumerConfiguration
    /// </summary>
    public PerQueueConsumerConfiguration(
        bool autoAck,
        string consumerTag,
        bool isExclusive,
        IDictionary<string, object>? arguments,
        ConsumeDelegate consumeDelegate
    )
    {
        AutoAck = autoAck;
        ConsumerTag = consumerTag;
        IsExclusive = isExclusive;
        Arguments = arguments;
        ConsumeDelegate = consumeDelegate;
    }

    /// <summary>
    ///     Indicates whether a consumer auto-acks messages
    /// </summary>
    public bool AutoAck { get; }

    /// <summary>
    ///     Tag of a consumer
    /// </summary>
    public string ConsumerTag { get; }

    /// <summary>
    ///     Indicates whether a consumer is exclusive
    /// </summary>
    public bool IsExclusive { get; }

    /// <summary>
    ///     Custom arguments
    /// </summary>
    public IDictionary<string, object>? Arguments { get; }

    /// <summary>
    ///     Handler for messages which are received by consumer
    /// </summary>
    public ConsumeDelegate ConsumeDelegate { get; }
}

/// <summary>
///     Configuration of the consumer
/// </summary>
public class ConsumerConfiguration
{
    /// <summary>
    ///     Creates ConsumerConfiguration
    /// </summary>
    public ConsumerConfiguration(
        ushort prefetchCount,
        IReadOnlyDictionary<Queue, PerQueueConsumerConfiguration> perQueueConfigurations
    )
    {
        PrefetchCount = prefetchCount;
        PerQueueConfigurations = perQueueConfigurations;
    }

    /// <summary>
    ///     PrefetchCount for the consumer
    /// </summary>
    public ushort PrefetchCount { get; }

    /// <summary>
    ///     Configurations of the consumer for queues
    /// </summary>
    public IReadOnlyDictionary<Queue, PerQueueConsumerConfiguration> PerQueueConfigurations { get; }
}

#pragma warning disable IDISP026
/// <inheritdoc />
public class Consumer : IConsumer
{
    private static readonly TimeSpan RestartConsumingPeriod = TimeSpan.FromSeconds(5);

    private readonly ConsumerConfiguration configuration;
    private readonly IEventBus eventBus;
    private readonly IInternalConsumerFactory internalConsumerFactory;
    private readonly IDisposable[] disposables;
    private readonly AsyncLock mutex = new();

    private volatile IInternalConsumer? consumer;
    private volatile bool disposed;

    /// <summary>
    ///     Creates Consumer
    /// </summary>
    public Consumer(
        ILogger<Consumer> logger,
        ConsumerConfiguration configuration,
        IInternalConsumerFactory internalConsumerFactory,
        IEventBus eventBus
    )
    {
        this.configuration = configuration;
        this.internalConsumerFactory = internalConsumerFactory;
        this.eventBus = eventBus;
        disposables =
        [
            eventBus.SubscribeAsync<ConnectionRecoveredEvent>((@event, _) => OnConnectionRecoveredAsync(@event, default)),
            eventBus.SubscribeAsync<ConnectionDisconnectedEvent>((@event, _) => OnConnectionDisconnectedAsync(@event, default)),
            Timers.StartAsync(RestartConsumingPeriodicallyAsync, RestartConsumingPeriod, RestartConsumingPeriod, logger)
        ];
    }

    /// <inheritdoc />
    public Guid Id { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public async Task StartConsumingAsync(CancellationToken cancellationToken = default)
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(Consumer));

        using (await mutex.AcquireAsync(cancellationToken))
        {
            if (consumer != null)
                throw new InvalidOperationException("Consumer has already started");

            consumer = internalConsumerFactory.CreateConsumer(configuration);
            consumer.CancelledAsync += InternalConsumerOnCancelledAsync;
        }

        var status = await consumer.StartConsumingAsync(cancellationToken: cancellationToken);
        foreach (var queue in status.Started)
            await eventBus.PublishAsync(new StartConsumingSucceededEvent(this, queue), cancellationToken);
        foreach (var queue in status.Failed)
            await eventBus.PublishAsync(new StartConsumingFailedEvent(this, queue), cancellationToken);
    }
    /// <inheritdoc />
    public virtual async ValueTask DisposeAsync()
    {
        if (disposed) return;

        disposed = true;

        var consumerToDispose = Interlocked.Exchange(ref consumer, null);
        if (consumerToDispose == null) return;

        foreach (var disposable in disposables)
            disposable.Dispose();

        await consumerToDispose.DisposeAsync();

        await eventBus.PublishAsync(new StoppedConsumingEvent(this));
        mutex.Dispose();
    }

    private async Task InternalConsumerOnCancelledAsync(object? sender, InternalConsumerCancelledEventArgs e)
    {
        if (e.Active.Count == 0)
            await DisposeAsync();
    }

    private async ValueTask OnConnectionDisconnectedAsync(ConnectionDisconnectedEvent @event, CancellationToken cancellationToken)
    {
        if (@event.Type != PersistentConnectionType.Consumer) return;

        if (consumer != null)
        {
            await consumer.StopConsumingAsync(cancellationToken);
        }
    }

    private async ValueTask OnConnectionRecoveredAsync(ConnectionRecoveredEvent @event, CancellationToken cancellationToken)
    {
        if (@event.Type != PersistentConnectionType.Consumer) return;

        var consumerToRestart = consumer;
        if (consumerToRestart == null) return;

        var status = await consumerToRestart.StartConsumingAsync(false, cancellationToken);

        foreach (var queue in status.Started)
            await eventBus.PublishAsync(new StartConsumingSucceededEvent(this, queue), cancellationToken);
        foreach (var queue in status.Failed)
            await eventBus.PublishAsync(new StartConsumingFailedEvent(this, queue), cancellationToken);

        if (ContainsOnlyFailedExclusiveQueues(status))
            await DisposeAsync();
    }

    private async Task  RestartConsumingPeriodicallyAsync(CancellationToken cancellationToken)
    {
        var consumerToRestart = consumer;
        if (consumerToRestart == null) return;

        var status = await consumerToRestart.StartConsumingAsync(false, cancellationToken);

        foreach (var queue in status.Started)
            await eventBus.PublishAsync(new StartConsumingSucceededEvent(this, queue), cancellationToken);
        foreach (var queue in status.Failed)
            await eventBus.PublishAsync(new StartConsumingFailedEvent(this, queue), cancellationToken);

        if (ContainsOnlyFailedExclusiveQueues(status))
            await DisposeAsync();
    }

    private static bool ContainsOnlyFailedExclusiveQueues(InternalConsumerStatus status)
    {
        return status.Active.Count == 0 && status.Failed.Count > 0 && status.Failed.All(x => x.IsExclusive);
    }
}
