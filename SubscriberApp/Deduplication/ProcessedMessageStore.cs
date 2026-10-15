namespace SubscriberApp.Deduplication;

// Remembers which messages already produced their effect.
//
// Keyed by message_id (the stored message), not delivery_id, so the same
// message is not applied twice even if it comes again in a new delivery.
//
// Persisted to a small file, so the critical scenario of increment 1
// ("subscriber crashes after the effect but before the ACK") is handled:
// after a restart the broker redelivers, the id is found here, and the
// subscriber only re-sends the ACK instead of applying the effect again.
public sealed class ProcessedMessageStore
{
    private readonly HashSet<string> _processed = new(StringComparer.Ordinal);
    private readonly object _sync = new();
    private readonly string _filePath;

    public ProcessedMessageStore(string filePath)
    {
        _filePath = filePath;

        if (File.Exists(_filePath))
        {
            foreach (string line in File.ReadAllLines(_filePath))
            {
                if (!string.IsNullOrWhiteSpace(line))
                {
                    _processed.Add(line.Trim());
                }
            }
        }
    }

    public int Count
    {
        get
        {
            lock (_sync)
            {
                return _processed.Count;
            }
        }
    }

    public bool HasProcessed(string messageId)
    {
        lock (_sync)
        {
            return _processed.Contains(messageId);
        }
    }

    // Called right after the effect, BEFORE the ACK is sent.
    public void MarkProcessed(string messageId)
    {
        lock (_sync)
        {
            if (_processed.Add(messageId))
            {
                File.AppendAllText(_filePath, messageId + Environment.NewLine);
            }
        }
    }
}
