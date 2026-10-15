using Grpc.Core;
using PublisherApp.Outbox;
using PublisherApp.Resilience;
using ReliableMessaging.Contracts.V1;

namespace PublisherApp;

// Thin wrapper over the generated gRPC client.
// Every call has a deadline; Publish is retried with the SAME idempotency key.
public sealed class PublisherClient
{
    // Shorter than the broker's fault-injection delay (5 s), so the
    // "response lost after commit" scenario surfaces as DEADLINE_EXCEEDED.
    public static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(2);

    private const int MaxPublishAttempts = 3;

    private readonly BrokerService.BrokerServiceClient _client;

    public PublisherClient(BrokerService.BrokerServiceClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    public async Task<RegisterPublisherReply> RegisterPublisherAsync(
        string publisherName,
        CancellationToken cancellationToken = default)
    {
        return await _client.RegisterPublisherAsync(
            new RegisterPublisherRequest { PublisherName = publisherName },
            deadline: DateTime.UtcNow.Add(CallDeadline),
            cancellationToken: cancellationToken).ResponseAsync;
    }

    public async Task<ServerInfo> GetServerInfoAsync(
        CancellationToken cancellationToken = default)
    {
        return await _client.GetServerInfoAsync(
            new GetServerInfoRequest(),
            deadline: DateTime.UtcNow.Add(CallDeadline),
            cancellationToken: cancellationToken).ResponseAsync;
    }

    // One attempt, used by the outbox worker.
    public async Task<PublishReply> PublishOnceAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var request = new PublishRequest
        {
            PublisherName = message.PublisherName,
            IdempotencyKey = message.IdempotencyKey,
            Content = message.Content
        };

        // A fresh deadline for every attempt.
        return await _client.PublishAsync(
            request,
            deadline: DateTime.UtcNow.Add(CallDeadline),
            cancellationToken: cancellationToken).ResponseAsync;
    }

    // Several attempts with backoff, always with the same idempotency key.
    public Task<PublishReply> PublishWithRetryAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        return RetryPolicy.ExecuteAsync(
            ct => PublishOnceAsync(message, ct),
            MaxPublishAttempts,
            TimeSpan.FromMilliseconds(500),
            cancellationToken,
            onRetry: (attempt, ex, delay) =>
                Console.WriteLine(
                    $"  [Retry] attempt {attempt} -> {ex.StatusCode} " +
                    "(outcome UNKNOWN: the broker may already have stored it). " +
                    $"Retrying in {delay.TotalMilliseconds:F0} ms with the SAME key " +
                    $"{message.IdempotencyKey}"));
    }
}
