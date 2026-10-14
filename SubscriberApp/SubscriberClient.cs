using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using ReliableMessaging.Contracts.V1;
using SubscriberApp.Deduplication;
using SubscriberApp.Ordering;

namespace SubscriberApp;

public sealed class SubscriberClient
{
    private readonly BrokerService.BrokerServiceClient _client;

    private readonly ProcessedMessageStore _processed;
    private readonly MessageOrderTracker _ordering;

    private readonly string _subscriberId;
    private readonly string _publisherName;

    public SubscriberClient(
        BrokerService.BrokerServiceClient client,
        string subscriberId,
        string publisherName)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _subscriberId = subscriberId ?? throw new ArgumentNullException(nameof(subscriberId));
        _publisherName = publisherName ?? throw new ArgumentNullException(nameof(publisherName));

        _processed = new ProcessedMessageStore();
        _ordering = new MessageOrderTracker();
    }

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeStreamAsync(cancellationToken);
            }
            catch (RpcException ex)
                when (ex.StatusCode is
                    StatusCode.Unavailable or
                    StatusCode.Cancelled or
                    StatusCode.DeadlineExceeded)
            {
                Console.WriteLine("Connection lost. Reconnecting...");
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    private async Task ConsumeStreamAsync(
        CancellationToken cancellationToken)
    {
        var request = new SubscribeRequest
        {
            SubscriberId = _subscriberId,
            PublisherName = _publisherName
        };

        using var call = _client.Subscribe(
            request,
            deadline: DateTime.UtcNow.AddMinutes(5),
            cancellationToken: cancellationToken);

        await foreach (var evt in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            if (evt.EventCase == SubscriberEvent.EventOneofCase.Started)
            {
                var started = evt.Started;

                Console.WriteLine(
                    $"Subscribed. Resumed={started.Resumed}, " +
                    $"Replayed={started.ReplayedMessages}, " +
                    $"Redelivered={started.RedeliveredMessages}");
            }
            else if (evt.EventCase == SubscriberEvent.EventOneofCase.Delivery)
            {
                var delivery = evt.Delivery;
                await HandleMessageAsync(delivery, cancellationToken);
            }
            else if (evt.EventCase == SubscriberEvent.EventOneofCase.Abandoned)
            {
                var abandoned = evt.Abandoned;

                Console.WriteLine(
                    $"Delivery abandoned: {abandoned.DeliveryId} seq #{abandoned.SequenceNumber} reason: {abandoned.Reason}");
            }
        }
    }

    private async Task HandleMessageAsync(
        Delivery delivery,
        CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"[{delivery.PublisherName}] Sequence: {delivery.SequenceNumber}");

        // Duplicate delivery.
        if (_processed.HasProcessed(delivery.DeliveryId))
        {
            Console.WriteLine($"Duplicate {delivery.DeliveryId}. Sending ACK.");
            await SendAckAsync(delivery.DeliveryId, cancellationToken);
            return;
        }

        // Out-of-order message.
        if (!_ordering.IsExpected(delivery.PublisherName, delivery.SequenceNumber))
        {
            Console.WriteLine($"Out-of-order message {delivery.SequenceNumber}. Buffering.");
            _ordering.Buffer(delivery.PublisherName, delivery);
            return;
        }

        await ProcessAndAckAsync(delivery, cancellationToken);

        // Check whether buffered messages can now be processed.
        var readyMessages = _ordering.TakeReadyMessages(delivery.PublisherName);

        foreach (var buffered in readyMessages)
        {
            await ProcessAndAckAsync(buffered, cancellationToken);
        }
    }

    private async Task ProcessAndAckAsync(
        Delivery delivery,
        CancellationToken cancellationToken)
    {
        try
        {
            // Simulate the local business operation.
            Console.WriteLine($"Processing: {delivery.Content}");
            await Task.Delay(100, cancellationToken);

            // Mark as processed BEFORE ACK (important for crash-recovery).
            _processed.TryMarkProcessed(delivery.DeliveryId);

            await SendAckAsync(delivery.DeliveryId, cancellationToken);

            Console.WriteLine($"ACK sent for {delivery.DeliveryId}.");
        }
        catch
        {
            await SendNackAsync(delivery.DeliveryId, cancellationToken);
            throw;
        }
    }

    private async Task SendAckAsync(
        string deliveryId,
        CancellationToken cancellationToken)
    {
        var request = new AckRequest
        {
            SubscriberId = _subscriberId,
            DeliveryId = deliveryId
        };

        // Await the unary response properly.
        await _client.AckAsync(
            request,
            deadline: DateTime.UtcNow.AddSeconds(3),
            cancellationToken: cancellationToken).ResponseAsync;
    }

    private async Task SendNackAsync(
        string deliveryId,
        CancellationToken cancellationToken)
    {
        var request = new NackRequest
        {
            SubscriberId = _subscriberId,
            DeliveryId = deliveryId
        };

        try
        {
            await _client.NackAsync(
                request,
                deadline: DateTime.UtcNow.AddSeconds(3),
                cancellationToken: cancellationToken).ResponseAsync;
        }
        catch (RpcException)
        {
            // Connection may already be gone.
        }
    }
}