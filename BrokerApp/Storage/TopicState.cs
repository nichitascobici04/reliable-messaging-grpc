using BrokerApp.Core;

namespace BrokerApp.Storage;

// One topic per publisher name (the "subject" used for routing).
// All fields are protected by Sync: checking the idempotency index,
// appending to the history and fanning out to subscribers must be
// ONE atomic step, which a single concurrent collection cannot give.
public sealed class TopicState
{
    public TopicState(string name)
    {
        Name = name;
    }

    public string Name { get; }

    public object Sync { get; } = new();

    // Full history, replayed to new subscribers (as in Part 1).
    public List<StoredMessage> History { get; } = new();

    // idempotency_key -> result of the first execution.
    public Dictionary<string, PublishRecord> IdempotencyIndex { get; } =
        new(StringComparer.Ordinal);

    public HashSet<string> SubscriberIds { get; } =
        new(StringComparer.Ordinal);
}