namespace BrokerApp.Storage;

// One delivery of one stored message to one subscriber.
// Lives in SubscriberSession.Pending until ACKed or dead-lettered.
public sealed class PendingDelivery
{
    public required string DeliveryId { get; init; }

    public required long SequenceNumber { get; init; }

    public required StoredMessage Message { get; init; }

    // Number of times it was actually handed to a stream.
    public int Attempts { get; set; }

    // null => not in flight (never sent, or subscriber disconnected).
    public DateTime? LastSentAtUtc { get; set; }

    public string? LastError { get; set; }

    public DateTime? DeadLetteredAtUtc { get; set; }
}