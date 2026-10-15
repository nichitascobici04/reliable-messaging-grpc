using Grpc.Core;

namespace PublisherApp.Resilience;

// Retry with exponential backoff + jitter.
// The SAME operation (same idempotency key) is repeated, so a retry
// after a lost response cannot create a second message on the broker.
public static class RetryPolicy
{
    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts,
        TimeSpan initialDelay,
        CancellationToken cancellationToken,
        Action<int, RpcException, TimeSpan>? onRetry = null)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await operation(cancellationToken);
            }
            catch (RpcException ex) when (IsTransient(ex) && attempt < maxAttempts)
            {
                double exponentialMs =
                    initialDelay.TotalMilliseconds * Math.Pow(2, attempt - 1);

                double jitterMs =
                    Random.Shared.NextDouble() * exponentialMs * 0.25;

                TimeSpan delay = TimeSpan.FromMilliseconds(exponentialMs + jitterMs);

                onRetry?.Invoke(attempt, ex, delay);

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    // DEADLINE_EXCEEDED / UNAVAILABLE => outcome UNKNOWN:
    // the broker may or may not have executed the call, so it is retried.
    // Everything else (INVALID_ARGUMENT, FAILED_PRECONDITION, ...) is final.
    public static bool IsTransient(RpcException ex) =>
        ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded;
}
