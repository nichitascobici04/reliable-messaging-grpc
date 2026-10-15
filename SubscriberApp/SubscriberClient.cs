using Grpc.Core;
using ReliableMessaging.Contracts.V1;
using SubscriberApp.Deduplication;
using SubscriberApp.Ordering;

namespace SubscriberApp;

// gRPC version of the Part 1 subscriber:
//  - Subscribe is a server stream (the broker pushes deliveries);
//  - ACK / NACK / DLQ / Unsubscribe are unary calls with a deadline;
//  - deduplication by message_id, ordering by sequence_number;
//  - automatic reconnection with the SAME subscriber_id, so the broker
//    resumes the session and redelivers whatever was not ACKed.
public sealed class SubscriberClient : IAsyncDisposable
{
    private static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan MaxReconnectDelay = TimeSpan.FromSeconds(15);

    // Keywords for demonstrations (same idea as "FAIL" in Part 1).
    private const string FailKeyword = "FAIL";
    private const string CrashKeyword = "#crash-before-ack";

    private readonly BrokerService.BrokerServiceClient _client;
    private readonly ProcessedMessageStore _processed;
    private readonly MessageOrderTracker _ordering = new();

    private CancellationTokenSource? _streamCts;
    private Task? _listenTask;

    public SubscriberClient(
        BrokerService.BrokerServiceClient client,
        string subscriberId)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        SubscriberId = subscriberId ?? throw new ArgumentNullException(nameof(subscriberId));
        _processed = new ProcessedMessageStore($"processed-{subscriberId}.txt");
    }

    public string SubscriberId { get; }

    public string? CurrentlySubscribedTo { get; private set; }

    // =========================================================
    // COMMANDS
    // =========================================================

    public async Task SubscribeAsync(string publisherName, CancellationToken appToken)
    {
        if (string.IsNullOrWhiteSpace(publisherName))
        {
            throw new ArgumentException("Publisher name is required.", nameof(publisherName));
        }

        // Only one stream per subscriber_id: close the old one first.
        await StopStreamAsync();

        _streamCts = CancellationTokenSource.CreateLinkedTokenSource(appToken);
        CancellationToken token = _streamCts.Token;

        CurrentlySubscribedTo = publisherName;
        _listenTask = Task.Run(() => RunStreamAsync(publisherName, token), CancellationToken.None);
    }

    public async Task UnsubscribeAsync(CancellationToken token)
    {
        try
        {
            // The broker completes our stream gracefully after this call.
            UnsubscribeReply reply = await _client.UnsubscribeAsync(
                new UnsubscribeRequest { SubscriberId = SubscriberId },
                deadline: DateTime.UtcNow.Add(CallDeadline),
                cancellationToken: token).ResponseAsync;

            Console.WriteLine(reply.WasSubscribed
                ? "Successfully unsubscribed."
                : "You were not subscribed.");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            Console.WriteLine("You were not subscribed.");
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"Unsubscribe failed: {ex.StatusCode} - {ex.Status.Detail}");
        }

        await StopStreamAsync();
        CurrentlySubscribedTo = null;
    }

    public async Task ShowDeadLettersAsync(CancellationToken token)
    {
        try
        {
            GetDeadLettersReply reply = await _client.GetDeadLettersAsync(
                new GetDeadLettersRequest { SubscriberId = SubscriberId },
                deadline: DateTime.UtcNow.Add(CallDeadline),
                cancellationToken: token).ResponseAsync;

            Console.WriteLine($"Dead Letter Queue: {reply.Entries.Count} message(s)");

            foreach (DeadLetter entry in reply.Entries)
            {
                Console.WriteLine(
                    $"  DLQ: [{entry.PublisherName}] seq #{entry.SequenceNumber} " +
                    $"\"{entry.Content}\" (attempts: {entry.Attempts}, " +
                    $"reason: {entry.LastError}, id: {entry.DeliveryId})");
            }

            Console.WriteLine("  --- End of DLQ ---");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            Console.WriteLine("The broker does not know this subscriber yet. Subscribe first.");
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"DLQ request failed: {ex.StatusCode} - {ex.Status.Detail}");
        }
    }

    public void PrintStatus()
    {
        Console.WriteLine($"Subscriber ID:      {SubscriberId}");
        Console.WriteLine($"Subscribed to:      {CurrentlySubscribedTo ?? "(none)"}");
        Console.WriteLine($"Next expected seq:  #{_ordering.NextExpected}");
        Console.WriteLine($"Buffered (gap):     {_ordering.BufferedCount}");
        Console.WriteLine($"Processed messages: {_processed.Count}");
    }

    public async ValueTask DisposeAsync()
    {
        await StopStreamAsync();
    }

    // =========================================================
    // STREAM + RECONNECTION
    // =========================================================

    private async Task RunStreamAsync(string publisherName, CancellationToken token)
    {
        int consecutiveFailures = 0;
        bool everStarted = false;

        while (!token.IsCancellationRequested)
        {
            try
            {
                var request = new SubscribeRequest
                {
                    SubscriberId = SubscriberId,
                    PublisherName = publisherName,
                    SkipHistory = false,

                    // First call = the user's "subscribe" command => NEW subscription,
                    // the broker replays the publisher's whole history.
                    // Automatic reconnects after a lost stream => RESUME, only the
                    // un-ACKed messages come again (no duplicates on screen).
                    Resume = everStarted
                };

                // No deadline on purpose: a subscription is long-lived.
                // It ends through cancellation (Ctrl+C / unsubscribe).
                using var call = _client.Subscribe(request, cancellationToken: token);

                await foreach (SubscriberEvent ev in call.ResponseStream.ReadAllAsync(token))
                {
                    switch (ev.EventCase)
                    {
                        case SubscriberEvent.EventOneofCase.Started:
                            everStarted = true;
                            consecutiveFailures = 0;
                            OnStarted(ev.Started);
                            break;

                        case SubscriberEvent.EventOneofCase.Delivery:
                            await HandleDeliveryAsync(ev.Delivery, token);
                            break;

                        case SubscriberEvent.EventOneofCase.Abandoned:
                            await HandleAbandonedAsync(ev.Abandoned, token);
                            break;
                    }
                }

                // Stream completed with OK => the broker closed it (unsubscribe).
                return;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (RpcException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Aborted)
            {
                Console.WriteLine(
                    "\n[Info] Another process took over this subscriber ID; this stream was closed.");
                return;
            }
            catch (RpcException ex) when (
                ex.StatusCode == StatusCode.InvalidArgument ||
                (ex.StatusCode == StatusCode.NotFound && !everStarted))
            {
                // Final answers: retrying the same request cannot succeed.
                Console.WriteLine($"\n[Subscribe failed] {ex.StatusCode}: {ex.Status.Detail}");
                CurrentlySubscribedTo = null;
                return;
            }
            catch (Exception ex)
            {
                // UNAVAILABLE (broker down / restarting), INTERNAL, or NOT_FOUND
                // after a broker restart before the publisher registered again.
                consecutiveFailures++;

                double backoffMs = Math.Min(
                    1000 * Math.Pow(2, consecutiveFailures - 1),
                    MaxReconnectDelay.TotalMilliseconds);

                TimeSpan delay = TimeSpan.FromMilliseconds(
                    backoffMs + Random.Shared.Next(0, 250));

                string reason = ex is RpcException rpc
                    ? $"{rpc.StatusCode} - {rpc.Status.Detail}"
                    : ex.Message;

                Console.WriteLine(
                    $"\n[Reconnect] Stream lost ({reason}). " +
                    $"Reconnecting in {delay.TotalSeconds:F1}s with the same subscriber ID " +
                    "(un-ACKed messages will be redelivered).");

                try
                {
                    await Task.Delay(delay, token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private void OnStarted(SubscriptionStarted started)
    {
        Console.WriteLine();

        if (started.Resumed)
        {
            Console.WriteLine(
                $"Session RESUMED on '{started.PublisherName}': " +
                $"{started.RedeliveredMessages} un-ACKed message(s) redelivered.");
        }
        else
        {
            Console.WriteLine(
                $"Successfully subscribed to '{started.PublisherName}': " +
                $"{started.ReplayedMessages} historical message(s) replayed.");

            // A new session numbers deliveries from wherever the broker starts.
            _ordering.ResyncOnNextDelivery();
        }

        Console.WriteLine(
            $"(ACK timeout {started.AckTimeoutSeconds}s, max {started.MaxAttempts} attempts)");
        Console.WriteLine();
    }

    // =========================================================
    // DELIVERY: dedup -> ordering -> process -> ACK
    // =========================================================

    private async Task HandleDeliveryAsync(Delivery delivery, CancellationToken token)
    {
        _ordering.ObserveFirstDelivery(delivery.SequenceNumber);

        if (await TrySettleDuplicateAsync(delivery, token))
        {
            await DrainBufferAsync(token);
            return;
        }

        switch (_ordering.Classify(delivery.SequenceNumber))
        {
            case SequenceStatus.Ahead:
                _ordering.Buffer(delivery);
                Console.WriteLine(
                    $"[Buffered] seq #{delivery.SequenceNumber} " +
                    $"(waiting for seq #{_ordering.NextExpected})");
                return;

            case SequenceStatus.Old:
                // A sequence number we already skipped (e.g. dead-lettered) - process it anyway.
                Console.WriteLine($"[Late] seq #{delivery.SequenceNumber} arrived after it was skipped.");
                await ProcessAsync(delivery, token);
                return;

            case SequenceStatus.Expected:
                if (await ProcessAsync(delivery, token))
                {
                    _ordering.MarkSettled(delivery.SequenceNumber);
                    await DrainBufferAsync(token);
                }
                return;
        }
    }

    private async Task HandleAbandonedAsync(DeliveryAbandoned abandoned, CancellationToken token)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"[DLQ] seq #{abandoned.SequenceNumber} was moved to the Dead Letter Queue " +
            $"({abandoned.Reason}). Skipping it so later messages are not blocked.");

        _ordering.MarkSettled(abandoned.SequenceNumber);
        await DrainBufferAsync(token);
    }

    // Already processed earlier (redelivery after a lost ACK, or after a crash):
    // do NOT apply the effect again, only confirm it.
    private async Task<bool> TrySettleDuplicateAsync(Delivery delivery, CancellationToken token)
    {
        if (!_processed.HasProcessed(delivery.MessageId))
        {
            return false;
        }

        // The message is still SHOWN (history replay must display every old
        // message), but its effect is not applied a second time.
        Console.WriteLine();
        Console.WriteLine(
            $"MESSAGE [{delivery.PublisherName}] seq #{delivery.SequenceNumber}: {delivery.Content}");
        Console.WriteLine(
            $"  already processed earlier by {SubscriberId} -> effect NOT applied again " +
            $"(delivery {delivery.DeliveryId}, attempt {delivery.Attempt})");

        await SendAckAsync(delivery, token);
        _ordering.MarkSettled(delivery.SequenceNumber);
        return true;
    }

    private async Task DrainBufferAsync(CancellationToken token)
    {
        Delivery? next;

        while ((next = _ordering.TakeNextReady()) is not null)
        {
            Console.WriteLine($"[Draining] buffered seq #{next.SequenceNumber}");

            if (await TrySettleDuplicateAsync(next, token))
            {
                continue;
            }

            if (!await ProcessAsync(next, token))
            {
                // NACKed: the broker redelivers it; later ones keep waiting.
                return;
            }

            _ordering.MarkSettled(next.SequenceNumber);
        }
    }

    // Returns true when the effect was applied and the message ACKed.
    private async Task<bool> ProcessAsync(Delivery delivery, CancellationToken token)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"MESSAGE [{delivery.PublisherName}] seq #{delivery.SequenceNumber}: {delivery.Content}");
        Console.WriteLine(
            $"  delivery {delivery.DeliveryId}, attempt {delivery.Attempt}");

        try
        {
            if (delivery.Content.Contains(FailKeyword, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Simulated local processing failure");
            }

            await Task.Delay(100, token); // the "work"

            // EFFECT is now applied and recorded (on disk) BEFORE the ACK.
            _processed.MarkProcessed(delivery.MessageId);

            if (delivery.Content.Contains(CrashKeyword, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine(
                    "  SIMULATED CRASH: the effect was applied but the ACK was NOT sent.");
                Console.WriteLine(
                    $"  Restart with: dotnet run --project SubscriberApp -- {SubscriberId}");
                Environment.Exit(1);
            }

            await SendAckAsync(delivery, token);
            return true;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            await SendNackAsync(delivery, ex.Message, token);
            return false;
        }
    }

    // =========================================================
    // ACK / NACK (unary calls with deadline)
    // =========================================================

    private async Task SendAckAsync(Delivery delivery, CancellationToken token)
    {
        const int maxAttempts = 3;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                AckReply reply = await _client.AckAsync(
                    new AckRequest
                    {
                        SubscriberId = SubscriberId,
                        DeliveryId = delivery.DeliveryId
                    },
                    deadline: DateTime.UtcNow.Add(CallDeadline),
                    cancellationToken: token).ResponseAsync;

                string note =
                    reply.AlreadyAcknowledged ? " (broker: already acknowledged - idempotent)" :
                    reply.RecoveredFromDeadLetter ? " (late ACK: removed from DLQ)" :
                    "";

                Console.WriteLine($"  ACK sent for seq #{delivery.SequenceNumber}{note}");
                return;
            }
            catch (RpcException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (RpcException ex) when (
                attempt < maxAttempts &&
                ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded)
            {
                await Task.Delay(300 * attempt, token);
            }
            catch (RpcException ex)
            {
                // Not fatal: the broker will redeliver, and the message will be
                // recognised as a duplicate and ACKed again.
                Console.WriteLine(
                    $"  ACK failed ({ex.StatusCode} - {ex.Status.Detail}). " +
                    "The broker will redeliver it; it will not be processed twice.");
                return;
            }
        }
    }

    private async Task SendNackAsync(Delivery delivery, string reason, CancellationToken token)
    {
        try
        {
            NackReply reply = await _client.NackAsync(
                new NackRequest
                {
                    SubscriberId = SubscriberId,
                    DeliveryId = delivery.DeliveryId,
                    Reason = reason
                },
                deadline: DateTime.UtcNow.Add(CallDeadline),
                cancellationToken: token).ResponseAsync;

            Console.WriteLine(reply.WillRetry
                ? $"  NACK sent ({reason}). Broker will retry " +
                  $"(attempt {reply.AttemptsSoFar}/{reply.MaxAttempts})."
                : $"  NACK sent ({reason}). No attempts left -> it goes to the DLQ.");
        }
        catch (RpcException ex) when (!token.IsCancellationRequested)
        {
            // Best effort: without the NACK the ACK timeout triggers the retry anyway.
            Console.WriteLine($"  NACK failed ({ex.StatusCode}); the ACK timeout will trigger a retry.");
        }
        catch (RpcException)
        {
        }
    }

    private async Task StopStreamAsync()
    {
        CancellationTokenSource? cts = _streamCts;
        Task? listenTask = _listenTask;

        _streamCts = null;
        _listenTask = null;

        if (cts is null)
        {
            return;
        }

        cts.Cancel();

        if (listenTask is not null)
        {
            try
            {
                await listenTask;
            }
            catch
            {
                // Already reported inside the loop.
            }
        }

        cts.Dispose();
    }
}
