using BrokerApp.Configuration;
using BrokerApp.Core;
using BrokerApp.Faults;
using BrokerApp.Storage;
using BrokerApp.Validation;
using Grpc.Core;
using Microsoft.Extensions.Options;
using ReliableMessaging.Contracts.V1;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace BrokerApp.Services;

// gRPC adapter: validates input, calls BrokerState, maps results to messages.
// Replaces HandleClient / HandlePublisherMessage / HandleSubscriberCommand
// and all manual "A|B|C" parsing from Part 1.
public sealed class BrokerGrpcService : BrokerService.BrokerServiceBase
{
    private readonly BrokerState state;
    private readonly FaultInjector faults;
    private readonly BrokerOptions options;
    private readonly ILogger<BrokerGrpcService> logger;

    public BrokerGrpcService(
        BrokerState state,
        FaultInjector faults,
        IOptions<BrokerOptions> options,
        ILogger<BrokerGrpcService> logger)
    {
        this.state = state;
        this.faults = faults;
        this.options = options.Value;
        this.logger = logger;
    }

    public override Task<ServerInfo> GetServerInfo(
        GetServerInfoRequest request,
        ServerCallContext context)
    {
        var info = new ServerInfo
        {
            ApiVersion = "v1",
            AckTimeoutSeconds = options.AckTimeoutSeconds,
            MaxRetries = options.MaxRetries,
            ServerTime = Timestamp.FromDateTime(DateTime.UtcNow)
        };

        info.SupportedVersions.Add("v1");

        return Task.FromResult(info);
    }

    public override Task<RegisterPublisherReply> RegisterPublisher(
        RegisterPublisherRequest request,
        ServerCallContext context)
    {
        RequestValidator.ValidatePublisherName(request.PublisherName);

        var (topic, created) = state.RegisterPublisher(request.PublisherName);

        int stored;

        lock (topic.Sync)
        {
            stored = topic.History.Count;
        }

        return Task.FromResult(new RegisterPublisherReply
        {
            PublisherName = topic.Name,
            AlreadyRegistered = !created,
            StoredMessages = stored
        });
    }

    public override async Task<PublishReply> Publish(
        PublishRequest request,
        ServerCallContext context)
    {
        RequestValidator.Validate(request);

        // The client already gave up (deadline / cancel): do not start the work.
        if (context.CancellationToken.IsCancellationRequested)
        {
            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "Request cancelled or deadline exceeded before processing; nothing was stored."));
        }

        PublishOutcome outcome =
            state.Publish(request.PublisherName, request.IdempotencyKey, request.Content);

        StoredMessage message = outcome.Record.Message;

        if (outcome.Status == PublishStatus.Duplicate)
        {
            logger.LogWarning(
                "Duplicate publish {Publisher}/{Key}: returning ORIGINAL result " +
                "(message {MessageId}, offset {Offset}). Nothing stored twice.",
                request.PublisherName, request.IdempotencyKey,
                message.MessageId, message.Offset);
        }
        else
        {
            logger.LogInformation(
                "Stored {MessageId} (offset {Offset}) from {Publisher}: \"{Content}\" " +
                "-> queued for {Count} subscriber(s)",
                message.MessageId, message.Offset, message.PublisherName,
                message.Content, outcome.Record.QueuedForSubscribers);
        }

        // POINT OF NO RETURN: the operation is committed.
        // Whatever happens to the response from here on is
        // exactly the ambiguity the client must handle.
        await faults.AfterPublishCommittedAsync(request, context);

        return new PublishReply
        {
            MessageId = message.MessageId,
            Offset = message.Offset,
            Duplicate = outcome.Status == PublishStatus.Duplicate,
            QueuedForSubscribers = outcome.Record.QueuedForSubscribers,
            AcceptedAt = Timestamp.FromDateTime(message.PublishedAtUtc)
        };
    }

    public override async Task Subscribe(
        SubscribeRequest request,
        IServerStreamWriter<SubscriberEvent> responseStream,
        ServerCallContext context)
    {
        RequestValidator.ValidateId(request.SubscriberId, "subscriber_id");
        RequestValidator.ValidatePublisherName(request.PublisherName);

        AttachResult attach =
            state.Attach(request.SubscriberId, request.PublisherName, request.SkipHistory);

        logger.LogInformation(
            "Subscriber {Subscriber} {Action} {Publisher}: {Replayed} replayed, {Redelivered} redelivered",
            request.SubscriberId,
            attach.Resumed ? "RESUMED" : "subscribed to",
            request.PublisherName, attach.Replayed, attach.Redelivered);

        // Stops when the client disconnects/cancels OR a newer stream takes over.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken, attach.StreamToken);

        try
        {
            await responseStream.WriteAsync(new SubscriberEvent
            {
                Started = new SubscriptionStarted
                {
                    SubscriberId = request.SubscriberId,
                    PublisherName = request.PublisherName,
                    Resumed = attach.Resumed,
                    ReplayedMessages = attach.Replayed,
                    RedeliveredMessages = attach.Redelivered,
                    AckTimeoutSeconds = options.AckTimeoutSeconds,
                    MaxAttempts = options.MaxAttempts
                }
            });

            await foreach (SubscriberEvent ev in
                attach.Channel.Reader.ReadAllAsync(linked.Token))
            {
                await responseStream.WriteAsync(ev);
            }

            logger.LogInformation(
                "Stream for {Subscriber} closed by broker (unsubscribed)",
                request.SubscriberId);
        }
        catch (OperationCanceledException)
            when (attach.StreamToken.IsCancellationRequested &&
                  !context.CancellationToken.IsCancellationRequested)
        {
            throw new RpcException(new Status(
                StatusCode.Aborted,
                "This stream was replaced by a newer Subscribe call with the same subscriber_id."));
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation(
                "Subscriber {Subscriber} disconnected; un-ACKed messages are kept for reconnect",
                request.SubscriberId);
        }
        catch (Exception) when (context.CancellationToken.IsCancellationRequested)
        {
            logger.LogInformation(
                "Subscriber {Subscriber} connection lost while writing",
                request.SubscriberId);
        }
        finally
        {
            state.Detach(attach.Session, attach.Channel);
        }
    }

    public override Task<AckReply> Ack(AckRequest request, ServerCallContext context)
    {
        RequestValidator.ValidateId(request.SubscriberId, "subscriber_id");
        RequestValidator.ValidateId(request.DeliveryId, "delivery_id");

        AckStatus status = state.Ack(request.SubscriberId, request.DeliveryId);

        return Task.FromResult(new AckReply
        {
            AlreadyAcknowledged = status == AckStatus.AlreadyAcknowledged,
            RecoveredFromDeadLetter = status == AckStatus.LateAckFromDeadLetter
        });
    }

    public override Task<NackReply> Nack(NackRequest request, ServerCallContext context)
    {
        RequestValidator.ValidateId(request.SubscriberId, "subscriber_id");
        RequestValidator.ValidateId(request.DeliveryId, "delivery_id");

        NackOutcome outcome =
            state.Nack(request.SubscriberId, request.DeliveryId, request.Reason);

        return Task.FromResult(new NackReply
        {
            WillRetry = outcome.WillRetry,
            AttemptsSoFar = outcome.AttemptsSoFar,
            MaxAttempts = options.MaxAttempts
        });
    }

    public override Task<UnsubscribeReply> Unsubscribe(
        UnsubscribeRequest request,
        ServerCallContext context)
    {
        RequestValidator.ValidateId(request.SubscriberId, "subscriber_id");

        bool wasSubscribed = state.Unsubscribe(request.SubscriberId);

        logger.LogInformation(
            "Subscriber {Subscriber} unsubscribed (was subscribed: {Was})",
            request.SubscriberId, wasSubscribed);

        return Task.FromResult(new UnsubscribeReply { WasSubscribed = wasSubscribed });
    }

    public override Task<GetDeadLettersReply> GetDeadLetters(
        GetDeadLettersRequest request,
        ServerCallContext context)
    {
        RequestValidator.ValidateId(request.SubscriberId, "subscriber_id");

        var reply = new GetDeadLettersReply();

        foreach (PendingDelivery dead in state.GetDeadLetters(request.SubscriberId))
        {
            reply.Entries.Add(new DeadLetter
            {
                DeliveryId = dead.DeliveryId,
                MessageId = dead.Message.MessageId,
                SequenceNumber = dead.SequenceNumber,
                PublisherName = dead.Message.PublisherName,
                Content = dead.Message.Content,
                Attempts = dead.Attempts,
                LastError = dead.LastError ?? "",
                DeadLetteredAt = Timestamp.FromDateTime(
                    dead.DeadLetteredAtUtc ?? DateTime.UtcNow)
            });
        }

        logger.LogInformation(
            "DLQ inspection for {Subscriber}: {Count} message(s)",
            request.SubscriberId, reply.Entries.Count);

        return Task.FromResult(reply);
    }
}