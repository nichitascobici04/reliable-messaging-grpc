namespace BrokerApp.Storage;

// A message accepted from a publisher. Immutable once stored.
public sealed class StoredMessage
{
    public required string MessageId { get; init; }

    public required long Offset { get; init; }

    public required string PublisherName { get; init; }

    public required string Content { get; init; }

    public required string IdempotencyKey { get; init; }

    public required DateTime PublishedAtUtc { get; init; }
}