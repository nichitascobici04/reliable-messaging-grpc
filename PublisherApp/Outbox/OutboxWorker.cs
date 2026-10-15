using Grpc.Core;
using PublisherApp.Resilience;

namespace PublisherApp.Outbox;

// Fallback path: messages whose outcome is unknown (broker down, response lost)
// stay in the local outbox file and are resent here, with their ORIGINAL
// idempotency key, as soon as the broker answers GetServerInfo again.
public sealed class OutboxWorker
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);

    // Messages younger than this are still being handled by the
    // foreground retry loop, so the worker leaves them alone.
    private static readonly TimeSpan MinimumAge = TimeSpan.FromSeconds(15);

    private readonly PublisherClient _publisher;
    private readonly FileOutbox _outbox;
    private readonly string _publisherName;

    private bool _brokerReachable = true;

    public OutboxWorker(
        PublisherClient publisher,
        FileOutbox outbox,
        string publisherName)
    {
        _publisher = publisher;
        _outbox = outbox;
        _publisherName = publisherName;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _publisher.GetServerInfoAsync(cancellationToken);

                if (!_brokerReachable)
                {
                    _brokerReachable = true;
                    Console.WriteLine("\n[Outbox] Broker is reachable again.");

                    // A restarted broker has lost its in-memory state:
                    // register again so subscribers can find this publisher.
                    await _publisher.RegisterPublisherAsync(_publisherName, cancellationToken);
                }

                await FlushAsync(cancellationToken);
            }
            catch (RpcException ex) when (RetryPolicy.IsTransient(ex))
            {
                if (_brokerReachable)
                {
                    _brokerReachable = false;
                    Console.WriteLine(
                        $"\n[Outbox] Broker unavailable ({ex.StatusCode}). " +
                        "New messages are kept in the outbox.");
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[Outbox] Unexpected error: {ex.Message}");
            }

            try
            {
                await Task.Delay(PollInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task FlushAsync(CancellationToken cancellationToken)
    {
        List<OutboxMessage> messages = await _outbox.GetAllAsync(cancellationToken);

        foreach (OutboxMessage message in messages
            .Where(m => DateTime.UtcNow - m.CreatedAtUtc >= MinimumAge)
            .OrderBy(m => m.CreatedAtUtc))
        {
            try
            {
                var reply = await _publisher.PublishOnceAsync(message, cancellationToken);

                await _outbox.RemoveAsync(message.IdempotencyKey, cancellationToken);

                Console.WriteLine(
                    $"\n[Outbox] \"{message.Content}\" delivered -> message {reply.MessageId}, " +
                    $"offset {reply.Offset}" +
                    (reply.Duplicate
                        ? " (duplicate: the broker had ALREADY stored it, nothing was stored twice)"
                        : ""));
            }
            catch (RpcException ex) when (RetryPolicy.IsTransient(ex))
            {
                // Still unknown: keep it and stop for this round to preserve order.
                return;
            }
            catch (RpcException ex)
            {
                // Final rejection (invalid content, key misuse): retrying cannot help.
                await _outbox.RemoveAsync(message.IdempotencyKey, cancellationToken);

                Console.WriteLine(
                    $"\n[Outbox] \"{message.Content}\" rejected by broker: " +
                    $"{ex.StatusCode} - {ex.Status.Detail}. Removed from outbox.");
            }
        }
    }
}
