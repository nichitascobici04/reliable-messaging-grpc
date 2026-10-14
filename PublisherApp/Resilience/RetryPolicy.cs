using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace PublisherApp.Resilience;

public static class RetryPolicy
{
    private static readonly Random Random = new();

    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts,
        TimeSpan initialDelay,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                if (attempt == maxAttempts)
                    throw;

                var exponentialDelay =
                    initialDelay.TotalMilliseconds *
                    Math.Pow(2, attempt - 1);

                var jitter =
                    Random.Shared.NextDouble() *
                    exponentialDelay * 0.25;

                var delay = TimeSpan.FromMilliseconds(
                    exponentialDelay + jitter);

                await Task.Delay(delay, cancellationToken);
            }
        }

        throw new InvalidOperationException(
            "Retry policy exited unexpectedly.");
    }

    private static bool IsTransient(Exception exception)
    {
        if (exception is Grpc.Core.RpcException rpc)
        {
            return rpc.StatusCode is
                Grpc.Core.StatusCode.Unavailable or
                Grpc.Core.StatusCode.DeadlineExceeded;
        }

        return false;
    }
}
