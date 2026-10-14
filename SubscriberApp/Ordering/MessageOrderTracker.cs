using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ReliableMessaging.Contracts.V1;

namespace SubscriberApp.Ordering;

// Simple in-memory ordering buffer for deliveries per publisher.
// Holds out-of-order deliveries and yields contiguous ready messages.
public sealed class MessageOrderTracker
{
    private readonly ConcurrentDictionary<string, long> _nextExpected =
        new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, SortedDictionary<long, Delivery>> _buffers =
        new(StringComparer.Ordinal);

    // Return true if the provided sequence is the expected one.
    public bool IsExpected(string publisherName, long sequenceNumber)
    {
        var expected = _nextExpected.GetOrAdd(publisherName, 1L);
        return sequenceNumber == expected;
    }

    // Buffer an out-of-order delivery.
    public void Buffer(string publisherName, Delivery delivery)
    {
        var buf = _buffers.GetOrAdd(publisherName, _ => new SortedDictionary<long, Delivery>());
        lock (buf)
        {
            if (!buf.ContainsKey(delivery.SequenceNumber))
                buf.Add(delivery.SequenceNumber, delivery);
        }
    }

    // Take and return contiguous ready messages starting from the current expected.
    // Advances the expected counter accordingly.
    public List<Delivery> TakeReadyMessages(string publisherName)
    {
        var ready = new List<Delivery>();

        var expected = _nextExpected.GetOrAdd(publisherName, 1L);
        if (!_buffers.TryGetValue(publisherName, out var buf))
            return ready;

        lock (buf)
        {
            while (buf.TryGetValue(expected, out var del))
            {
                buf.Remove(expected);
                ready.Add(del);
                expected++;
            }
        }

        _nextExpected[publisherName] = expected;
        return ready;
    }

    // When a message is processed, caller should advance next expected if it was the exact next.
    // This helper can be used by higher level logic if needed.
    public void AdvanceExpected(string publisherName)
    {
        _nextExpected.AddOrUpdate(publisherName, 2L, (_, prev) => prev + 1);
    }
}