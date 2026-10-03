using System.Collections.Concurrent;
using BrokerApp.Configuration;
using Grpc.Core;
using Microsoft.Extensions.Options;
using ReliableMessaging.Contracts.V1;

namespace BrokerApp.Faults;

// Reproduces the critical scenario of increment 2:
// the remote operation IS performed, but the response is lost.
// Only the FIRST attempt for a given idempotency key is affected,
// so a correct client retry succeeds (as a deduplicated duplicate).
public sealed class FaultInjector
{
    private readonly FaultInjectionOptions options;
    private readonly ILogger<FaultInjector> logger;

    private readonly ConcurrentDictionary<string, byte> injected =
        new(StringComparer.Ordinal);

    public FaultInjector(
        IOptions<BrokerOptions> options,
        ILogger<FaultInjector> logger)
    {
        this.options = options.Value.FaultInjection;
        this.logger = logger;
    }

    public async Task AfterPublishCommittedAsync(
        PublishRequest request,
        ServerCallContext context)
    {
        if (!options.Enabled)
        {
            return;
        }

        bool requested =
            !string.IsNullOrEmpty(options.LoseResponseKeyword) &&
            request.Content.Contains(
                options.LoseResponseKeyword,
                StringComparison.OrdinalIgnoreCase);

        bool random =
            options.LostResponseProbability > 0 &&
            Random.Shared.NextDouble() < options.LostResponseProbability;

        if (!requested && !random)
        {
            return;
        }

        if (!injected.TryAdd($"{request.PublisherName}/{request.IdempotencyKey}", 0))
        {
            return; // already injected once for this key: let the retry through
        }

        logger.LogWarning(
            "FAULT INJECTION: message from {Publisher} (key {Key}) is COMMITTED, " +
            "but its response will be lost (mode {Mode}).",
            request.PublisherName, request.IdempotencyKey, options.Mode);

        if (options.Mode == LostResponseMode.Abort)
        {
            throw new RpcException(new Status(
                StatusCode.Unavailable,
                "Simulated connection loss after the operation was performed."));
        }

        try
        {
            await Task.Delay(options.DelayMs, context.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Client stopped waiting (deadline/cancel) for key {Key}. " +
                "The message stays stored; a retry with the same key will be deduplicated.",
                request.IdempotencyKey);

            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "Client is gone."));
        }

        logger.LogWarning(
            "Response for key {Key} sent {Delay} ms late. " +
            "A client without a deadline was blocked all this time.",
            request.IdempotencyKey, options.DelayMs);
    }
}