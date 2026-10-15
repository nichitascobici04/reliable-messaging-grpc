using ReliableMessaging.Contracts.V1;

namespace SubscriberApp.Ordering;

// Delivers messages to the application strictly in sequence_number order.
//
// The broker numbers deliveries PER SUBSCRIBER SESSION (1, 2, 3, ...),
// across all publishers the session has been subscribed to, so a single
// counter is kept here (not one per publisher).
//
// Used only from the stream-reading loop, so it is not shared between threads.
public sealed class MessageOrderTracker
{
    // Deliveries that arrived early. A null value means "this sequence number
    // is already settled" (dead-lettered or already processed) and can be skipped.
    private readonly SortedDictionary<long, Delivery?> _waiting = new();

    private bool _resyncOnNextDelivery = true;

    public long NextExpected { get; private set; } = 1;

    public int BufferedCount => _waiting.Count(kv => kv.Value is not null);

    // A NEW (not resumed) session starts numbering wherever the broker says;
    // the first delivery after SubscriptionStarted defines the starting point.
    public void ResyncOnNextDelivery()
    {
        _resyncOnNextDelivery = true;
    }

    public void ObserveFirstDelivery(long sequenceNumber)
    {
        if (!_resyncOnNextDelivery)
        {
            return;
        }

        _resyncOnNextDelivery = false;
        _waiting.Clear();
        NextExpected = sequenceNumber;
    }

    public SequenceStatus Classify(long sequenceNumber)
    {
        if (sequenceNumber < NextExpected)
        {
            return SequenceStatus.Old;
        }

        return sequenceNumber == NextExpected
            ? SequenceStatus.Expected
            : SequenceStatus.Ahead;
    }

    public void Buffer(Delivery delivery)
    {
        // Already settled: nothing left to do for this sequence number.
        if (_waiting.TryGetValue(delivery.SequenceNumber, out Delivery? existing) &&
            existing is null)
        {
            return;
        }

        // Keep the newest copy (higher attempt number) of the same delivery.
        _waiting[delivery.SequenceNumber] = delivery;
    }

    // The sequence number is settled: processed, or abandoned to the DLQ.
    public void MarkSettled(long sequenceNumber)
    {
        if (sequenceNumber == NextExpected)
        {
            NextExpected++;
        }
        else if (sequenceNumber > NextExpected)
        {
            _waiting[sequenceNumber] = null;
        }
    }

    // Next buffered delivery that is now in order, or null if there is a gap.
    // Settled placeholders are skipped automatically.
    public Delivery? TakeNextReady()
    {
        while (_waiting.TryGetValue(NextExpected, out Delivery? delivery))
        {
            _waiting.Remove(NextExpected);

            if (delivery is null)
            {
                NextExpected++;
                continue;
            }

            return delivery;
        }

        return null;
    }
}

public enum SequenceStatus
{
    Old,
    Expected,
    Ahead
}
