using System.Threading.Channels;
using ReliableMessaging.Contracts.V1;

namespace BrokerApp.Storage;

// Replaces ClientConnection from Part 1. A session survives a dropped
// stream, so a subscriber that reconnects with the same subscriber_id
// gets its un-ACKed messages again instead of losing them.
// All mutable fields are protected by Sync.
public sealed class SubscriberSession
{
    public SubscriberSession(string subscriberId)
    {
        SubscriberId = subscriberId;
    }

    public string SubscriberId { get; }

    public object Sync { get; } = new();

    public string? PublisherName { get; set; }

    public long NextSequence { get; set; } = 1;

    public Dictionary<string, PendingDelivery> Pending { get; } =
        new(StringComparer.Ordinal);

    public List<PendingDelivery> DeadLetters { get; } = new();

    // Lets a repeated ACK be answered as "already acknowledged".
    public HashSet<string> AckedDeliveryIds { get; } =
        new(StringComparer.Ordinal);

    // Thread-safe queue between the broker logic (many writers)
    // and the gRPC response stream (single reader).
    // null => no active stream.
    public Channel<SubscriberEvent>? Outbox { get; set; }

    public CancellationTokenSource? StreamCts { get; set; }

    public DateTime LastSeenUtc { get; set; } = DateTime.UtcNow;

    public bool IsConnected => Outbox is not null;
}