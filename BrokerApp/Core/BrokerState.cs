using System.Collections.Concurrent;
using System.Threading.Channels;
using BrokerApp.Configuration;
using BrokerApp.Storage;
using Grpc.Core;
using Microsoft.Extensions.Options;
using ReliableMessaging.Contracts.V1;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace BrokerApp.Core;

// Transport-independent broker logic (routing, storage, retry, DLQ).
//
// Concurrency model:
//  - topics / sessions: ConcurrentDictionary (thread-safe registries)
//  - per-topic lock:    idempotency check + append + fan-out are atomic,
//                       which also keeps per-topic ordering
//  - per-session lock:  pending / DLQ / sequence counter
//  - lock order is always topic -> session (never the reverse) => no deadlocks
public sealed class BrokerState
{
    private readonly ConcurrentDictionary<string, TopicState> topics =
        new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, SubscriberSession> sessions =
        new(StringComparer.Ordinal);

    private readonly BrokerOptions options;
    private readonly ILogger<BrokerState> logger;

    public BrokerState(
        IOptions<BrokerOptions> options,
        ILogger<BrokerState> logger)
    {
        this.options = options.Value;
        this.logger = logger;
    }

    // =========================================================
    // PUBLISHERS
    // =========================================================

    public (TopicState Topic, bool Created) RegisterPublisher(string publisherName)
    {
        if (topics.TryGetValue(publisherName, out TopicState? existing))
        {
            return (existing, false);
        }

        var created = new TopicState(publisherName);

        if (topics.TryAdd(publisherName, created))
        {
            logger.LogInformation(
                "Publisher registered: {Publisher}", publisherName);

            return (created, true);
        }

        // Another call registered it at the same moment.
        return (topics[publisherName], false);
    }

    public PublishOutcome Publish(
        string publisherName,
        string idempotencyKey,
        string content)
    {
        var (topic, _) = RegisterPublisher(publisherName);

        lock (topic.Sync)
        {
            // Same key seen before => same operation, retried by the client.
            if (topic.IdempotencyIndex.TryGetValue(idempotencyKey, out PublishRecord? previous))
            {
                if (!string.Equals(previous.Message.Content, content, StringComparison.Ordinal))
                {
                    throw new BrokerException(
                        StatusCode.FailedPrecondition,
                        $"idempotency_key '{idempotencyKey}' was already used for a different " +
                        $"message (message_id {previous.Message.MessageId}). " +
                        "Generate a new key for a new message.");
                }

                return new PublishOutcome(previous, PublishStatus.Duplicate);
            }

            var message = new StoredMessage
            {
                MessageId = Guid.NewGuid().ToString("N"),
                Offset = topic.History.Count + 1,
                PublisherName = publisherName,
                Content = content,
                IdempotencyKey = idempotencyKey,
                PublishedAtUtc = DateTime.UtcNow
            };

            topic.History.Add(message);

            int queued = 0;

            foreach (string subscriberId in topic.SubscriberIds)
            {
                if (!sessions.TryGetValue(subscriberId, out SubscriberSession? session))
                {
                    continue;
                }

                lock (session.Sync)
                {
                    EnqueueNewDelivery(session, message);
                }

                queued++;
            }

            var record = new PublishRecord(message, queued);

            topic.IdempotencyIndex[idempotencyKey] = record;

            return new PublishOutcome(record, PublishStatus.Created);
        }
    }

    // =========================================================
    // SUBSCRIBERS
    // =========================================================

    // resumeRequested = false => explicit "subscribe": a NEW subscription that
    //                           replays the whole history (as in Part 1).
    // resumeRequested = true  => automatic reconnect after a dropped stream:
    //                           only the un-ACKed messages are sent again.
    public AttachResult Attach(
        string subscriberId,
        string publisherName,
        bool skipHistory,
        bool resumeRequested)
    {
        if (!topics.TryGetValue(publisherName, out TopicState? topic))
        {
            throw new BrokerException(
                StatusCode.NotFound,
                $"Unknown publisher '{publisherName}'. " +
                "A publisher must register or publish first.");
        }

        SubscriberSession session =
            sessions.GetOrAdd(subscriberId, id => new SubscriberSession(id));

        // Switching to another publisher: leave the old topic first.
        string? previousTopicName;

        lock (session.Sync)
        {
            previousTopicName = session.PublisherName;
        }

        if (previousTopicName is not null &&
            previousTopicName != publisherName &&
            topics.TryGetValue(previousTopicName, out TopicState? previousTopic))
        {
            lock (previousTopic.Sync)
            {
                previousTopic.SubscriberIds.Remove(subscriberId);
            }
        }

        var channel = Channel.CreateUnbounded<SubscriberEvent>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        var streamCts = new CancellationTokenSource();

        CancellationTokenSource? replacedStream;
        bool resumed;
        int redelivered = 0;
        int replayed = 0;

        lock (topic.Sync)
            lock (session.Sync)
            {
                // A resume is only possible for the same publisher, and only
                // when the client asks for it (reconnect after a lost stream).
                resumed = resumeRequested && session.PublisherName == publisherName;

                // Only one active stream per subscriber_id.
                replacedStream = session.StreamCts;
                session.Outbox?.Writer.TryComplete();

                session.Outbox = channel;
                session.StreamCts = streamCts;
                session.PublisherName = publisherName;
                session.LastSeenUtc = DateTime.UtcNow;

                topic.SubscriberIds.Add(subscriberId);

                if (resumed)
                {
                    // RESUME: everything still un-ACKed goes out again, in order.
                    foreach (PendingDelivery pending in
                        session.Pending.Values.OrderBy(p => p.SequenceNumber).ToList())
                    {
                        TrySend(session, pending);
                        redelivered++;
                    }
                }
                else if (!skipHistory)
                {
                    // NEW subscription: the old un-ACKed deliveries are replaced by
                    // the full history (which already contains those messages),
                    // so nothing is sent twice. The DLQ is kept for inspection.
                    session.Pending.Clear();

                    foreach (StoredMessage stored in topic.History)
                    {
                        EnqueueNewDelivery(session, stored);
                        replayed++;
                    }
                }
                else
                {
                    // New subscription without history: keep only what is still
                    // pending for this publisher.
                    foreach (PendingDelivery pending in session.Pending.Values
                        .Where(p => p.Message.PublisherName != publisherName)
                        .ToList())
                    {
                        session.Pending.Remove(pending.DeliveryId);
                    }

                    foreach (PendingDelivery pending in
                        session.Pending.Values.OrderBy(p => p.SequenceNumber).ToList())
                    {
                        TrySend(session, pending);
                        redelivered++;
                    }
                }
            }

        replacedStream?.Cancel();

        return new AttachResult(
            session, channel, streamCts.Token, resumed, replayed, redelivered);
    }

    // Called when a Subscribe stream ends for any reason.
    public void Detach(SubscriberSession session, Channel<SubscriberEvent> channel)
    {
        lock (session.Sync)
        {
            // A newer stream already took over: nothing to do.
            if (!ReferenceEquals(session.Outbox, channel))
            {
                return;
            }

            session.Outbox = null;
            session.StreamCts = null;
            session.LastSeenUtc = DateTime.UtcNow;

            // In-flight messages are no longer in flight;
            // they will be resent when the subscriber reconnects.
            foreach (PendingDelivery pending in session.Pending.Values)
            {
                pending.LastSentAtUtc = null;
            }
        }
    }

    public bool Unsubscribe(string subscriberId)
    {
        SubscriberSession session = GetSession(subscriberId);

        string? topicName;

        lock (session.Sync)
        {
            topicName = session.PublisherName;
            session.LastSeenUtc = DateTime.UtcNow;
        }

        if (topicName is null)
        {
            return false;
        }

        if (topics.TryGetValue(topicName, out TopicState? topic))
        {
            lock (topic.Sync)
                lock (session.Sync)
                {
                    topic.SubscriberIds.Remove(subscriberId);
                    session.PublisherName = null;

                    // Completing the channel ends the stream gracefully (status OK).
                    session.Outbox?.Writer.TryComplete();
                }
        }

        return true;
    }

    // =========================================================
    // ACK / NACK / DLQ
    // =========================================================

    public AckStatus Ack(string subscriberId, string deliveryId)
    {
        SubscriberSession session = GetSession(subscriberId);

        lock (session.Sync)
        {
            session.LastSeenUtc = DateTime.UtcNow;

            if (session.Pending.Remove(deliveryId, out PendingDelivery? pending))
            {
                session.AckedDeliveryIds.Add(deliveryId);

                logger.LogInformation(
                    "ACK {Delivery} (seq #{Seq}) from {Subscriber} after {Attempts} attempt(s)",
                    deliveryId, pending.SequenceNumber, subscriberId, pending.Attempts);

                return AckStatus.Acknowledged;
            }

            if (session.AckedDeliveryIds.Contains(deliveryId))
            {
                logger.LogInformation(
                    "Duplicate ACK {Delivery} from {Subscriber}: already acknowledged (idempotent)",
                    deliveryId, subscriberId);

                return AckStatus.AlreadyAcknowledged;
            }

            int index = session.DeadLetters.FindIndex(d => d.DeliveryId == deliveryId);

            if (index >= 0)
            {
                session.DeadLetters.RemoveAt(index);
                session.AckedDeliveryIds.Add(deliveryId);

                logger.LogWarning(
                    "Late ACK {Delivery} from {Subscriber}: removed from DLQ",
                    deliveryId, subscriberId);

                return AckStatus.LateAckFromDeadLetter;
            }
        }

        throw new BrokerException(
            StatusCode.NotFound,
            $"Unknown delivery_id '{deliveryId}' for subscriber '{subscriberId}'.");
    }

    public NackOutcome Nack(string subscriberId, string deliveryId, string reason)
    {
        SubscriberSession session = GetSession(subscriberId);

        lock (session.Sync)
        {
            session.LastSeenUtc = DateTime.UtcNow;

            if (session.Pending.TryGetValue(deliveryId, out PendingDelivery? pending))
            {
                pending.LastError = string.IsNullOrWhiteSpace(reason) ? "NACK" : reason;

                // Make it due immediately for the retry worker.
                pending.LastSentAtUtc =
                    DateTime.UtcNow.AddSeconds(-options.AckTimeoutSeconds);

                bool willRetry = pending.Attempts < options.MaxAttempts;

                logger.LogWarning(
                    "NACK {Delivery} (seq #{Seq}) from {Subscriber}: {Reason}. {Next}",
                    deliveryId, pending.SequenceNumber, subscriberId, pending.LastError,
                    willRetry ? "Will be retried." : "No retries left, goes to DLQ.");

                return new NackOutcome(willRetry, pending.Attempts);
            }

            if (session.AckedDeliveryIds.Contains(deliveryId))
            {
                throw new BrokerException(
                    StatusCode.FailedPrecondition,
                    $"Delivery '{deliveryId}' was already acknowledged; NACK ignored.");
            }
        }

        throw new BrokerException(
            StatusCode.NotFound,
            $"Unknown delivery_id '{deliveryId}' for subscriber '{subscriberId}'.");
    }

    public IReadOnlyList<PendingDelivery> GetDeadLetters(string subscriberId)
    {
        SubscriberSession session = GetSession(subscriberId);

        lock (session.Sync)
        {
            return session.DeadLetters.ToList();
        }
    }

    // =========================================================
    // RETRY CYCLE (called periodically by RetryWorker)
    // =========================================================

    public void RunRetryCycle()
    {
        DateTime now = DateTime.UtcNow;
        var expired = new List<SubscriberSession>();

        foreach (SubscriberSession session in sessions.Values)
        {
            lock (session.Sync)
            {
                if (!session.IsConnected)
                {
                    // Disconnected subscribers do not burn retries.
                    if (now - session.LastSeenUtc >
                        TimeSpan.FromMinutes(options.SessionExpiryMinutes))
                    {
                        expired.Add(session);
                    }

                    continue;
                }

                foreach (PendingDelivery pending in
                    session.Pending.Values.OrderBy(p => p.SequenceNumber).ToList())
                {
                    bool inFlight =
                        pending.LastSentAtUtc is DateTime sentAt &&
                        (now - sentAt).TotalSeconds < options.AckTimeoutSeconds;

                    if (inFlight)
                    {
                        continue;
                    }

                    bool timedOut = pending.LastSentAtUtc is not null;

                    if (timedOut && pending.Attempts >= options.MaxAttempts)
                    {
                        MoveToDeadLetter(session, pending, now);
                        continue;
                    }

                    if (TrySend(session, pending) && pending.Attempts > 1)
                    {
                        logger.LogWarning(
                            "Retry {Retry}/{Max} for {Delivery} (seq #{Seq}) -> {Subscriber}",
                            pending.Attempts - 1, options.MaxRetries,
                            pending.DeliveryId, pending.SequenceNumber,
                            session.SubscriberId);
                    }
                }
            }
        }

        foreach (SubscriberSession session in expired)
        {
            RemoveSession(session);
        }
    }

    // =========================================================
    // HELPERS (callers hold session.Sync where noted)
    // =========================================================

    // Caller holds session.Sync.
    private void EnqueueNewDelivery(SubscriberSession session, StoredMessage message)
    {
        var pending = new PendingDelivery
        {
            DeliveryId = Guid.NewGuid().ToString("N"),
            SequenceNumber = session.NextSequence++,
            Message = message
        };

        // Store BEFORE sending (same rule as Part 1): if the send fails,
        // the retry worker still knows about the message.
        session.Pending[pending.DeliveryId] = pending;

        TrySend(session, pending);
    }

    // Caller holds session.Sync.
    private static bool TrySend(SubscriberSession session, PendingDelivery pending)
    {
        if (session.Outbox is null)
        {
            pending.LastSentAtUtc = null;
            return false;
        }

        pending.Attempts++;

        if (session.Outbox.Writer.TryWrite(ToEvent(pending)))
        {
            pending.LastSentAtUtc = DateTime.UtcNow;
            return true;
        }

        // Channel already completed (stream closing).
        pending.Attempts--;
        pending.LastSentAtUtc = null;
        return false;
    }

    // Caller holds session.Sync.
    private void MoveToDeadLetter(
        SubscriberSession session,
        PendingDelivery pending,
        DateTime now)
    {
        session.Pending.Remove(pending.DeliveryId);

        pending.DeadLetteredAtUtc = now;
        pending.LastError ??= $"No ACK after {pending.Attempts} attempt(s)";

        session.DeadLetters.Add(pending);

        // Tell the subscriber so its ordering logic can skip this sequence number.
        session.Outbox?.Writer.TryWrite(new SubscriberEvent
        {
            Abandoned = new DeliveryAbandoned
            {
                DeliveryId = pending.DeliveryId,
                MessageId = pending.Message.MessageId,
                SequenceNumber = pending.SequenceNumber,
                Reason = pending.LastError
            }
        });

        logger.LogError(
            "Delivery {Delivery} (seq #{Seq}) for {Subscriber} moved to DLQ: {Reason}",
            pending.DeliveryId, pending.SequenceNumber,
            session.SubscriberId, pending.LastError);
    }

    private void RemoveSession(SubscriberSession session)
    {
        string? topicName;

        lock (session.Sync)
        {
            if (session.IsConnected)
            {
                return; // reconnected meanwhile
            }

            topicName = session.PublisherName;
        }

        if (topicName is not null &&
            topics.TryGetValue(topicName, out TopicState? topic))
        {
            lock (topic.Sync)
            {
                topic.SubscriberIds.Remove(session.SubscriberId);
            }
        }

        sessions.TryRemove(
            new KeyValuePair<string, SubscriberSession>(session.SubscriberId, session));

        logger.LogInformation(
            "Session {Subscriber} expired and was removed", session.SubscriberId);
    }

    private SubscriberSession GetSession(string subscriberId)
    {
        return sessions.TryGetValue(subscriberId, out SubscriberSession? session)
            ? session
            : throw new BrokerException(
                StatusCode.NotFound,
                $"Unknown subscriber_id '{subscriberId}'. Call Subscribe first.");
    }

    private static SubscriberEvent ToEvent(PendingDelivery pending)
    {
        return new SubscriberEvent
        {
            Delivery = new Delivery
            {
                DeliveryId = pending.DeliveryId,
                MessageId = pending.Message.MessageId,
                SequenceNumber = pending.SequenceNumber,
                PublisherName = pending.Message.PublisherName,
                Content = pending.Message.Content,
                Attempt = pending.Attempts,
                Offset = pending.Message.Offset,
                PublishedAt = Timestamp.FromDateTime(pending.Message.PublishedAtUtc)
            }
        };
    }
}