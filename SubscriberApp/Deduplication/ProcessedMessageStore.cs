using System.Collections.Concurrent;

namespace SubscriberApp.Deduplication;

public sealed class ProcessedMessageStore
{
    private readonly ConcurrentDictionary<string, byte> _processed = new();

    public bool HasProcessed(string deliveryId)
    {
        return _processed.ContainsKey(deliveryId);
    }

    public bool TryMarkProcessed(string deliveryId)
    {
        return _processed.TryAdd(deliveryId, 0);
    }
}