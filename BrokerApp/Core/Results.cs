using System.Threading.Channels;
using BrokerApp.Storage;
using ReliableMessaging.Contracts.V1;

namespace BrokerApp.Core;

public enum PublishStatus
{
    Created,
    Duplicate
}

public sealed record PublishRecord(
    StoredMessage Message,
    int QueuedForSubscribers);

public sealed record PublishOutcome(
    PublishRecord Record,
    PublishStatus Status);

public sealed record AttachResult(
    SubscriberSession Session,
    Channel<SubscriberEvent> Channel,
    CancellationToken StreamToken,
    bool Resumed,
    int Replayed,
    int Redelivered);

public enum AckStatus
{
    Acknowledged,
    AlreadyAcknowledged,
    LateAckFromDeadLetter
}

public sealed record NackOutcome(
    bool WillRetry,
    int AttemptsSoFar);