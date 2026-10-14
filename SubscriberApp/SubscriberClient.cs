using ReliableMessaging.Contracts.V1;
using Grpc.Core;
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
        MessageBroker.MessageBrokerClient client,
        string subscriberId,
        string publisherId)
    {
        _client = client;
        _subscriberId = subscriberId;
        _publisherId = publisherId;

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
                Console.WriteLine(
                    "Connection lost. Reconnecting...");
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(2),
                    cancellationToken);
            }
        }
    }

    private async Task ConsumeStreamAsync(
        CancellationToken cancellationToken)
    {
        var request = new SubscribeRequest
        {
            SubscriberId = _subscriberId,
            PublisherId = _publisherId
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

                // procesarea mesajului
            }
            else if (evt.EventCase == SubscriberEvent.EventOneofCase.Abandoned)
            {
                var abandoned = evt.Abandoned;

                // mesajul a ajuns în DLQ
            }
        }
    }
    private async Task HandleMessageAsync(
    Message message,
    CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"[{message.PublisherId}] " +
            $"Sequence: {message.SequenceNumber}");

        /*
         * Duplicate delivery.
         */
        if (_processed.HasProcessed(message.DeliveryId))
        {
            Console.WriteLine(
                $"Duplicate {message.DeliveryId}. Sending ACK.");

            await SendAckAsync(
                message.DeliveryId,
                cancellationToken);

            return;
        }

        /*
         * Out-of-order message.
         */
        if (!_ordering.IsExpected(
                message.PublisherId,
                message.SequenceNumber))
        {
            Console.WriteLine(
                $"Out-of-order message {message.SequenceNumber}. " +
                "Buffering.");

            _ordering.Buffer(
                message.PublisherId,
                message);

            return;
        }

        await ProcessAndAckAsync(
            message,
            cancellationToken);

        /*
         * Check whether buffered messages can now be processed.
         */
        var readyMessages =
            _ordering.TakeReadyMessages(
                message.PublisherId);

        foreach (var buffered in readyMessages)
        {
            await ProcessAndAckAsync(
                buffered,
                cancellationToken);
        }
    }

    private async Task ProcessAndAckAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        try
        {
            /*
             * Simulate the local business operation.
             */
            Console.WriteLine(
                $"Processing: {message.Payload}");

            await Task.Delay(100, cancellationToken);

            /*
             * Mark as processed BEFORE ACK.
             *
             * This is important for the critical failure scenario.
             */
            _processed.TryMarkProcessed(
                message.DeliveryId);

            await SendAckAsync(
                message.DeliveryId,
                cancellationToken);

            Console.WriteLine(
                $"ACK sent for {message.DeliveryId}.");
        }
        catch
        {
            await SendNackAsync(
                message.DeliveryId,
                cancellationToken);

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

        await _client.AckAsync(
            request,
            deadline: DateTime.UtcNow.AddSeconds(3),
            cancellationToken: cancellationToken);
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
                cancellationToken: cancellationToken);
        }
        catch (RpcException)
        {
            // Connection may already be gone.
        }
    }
}