using BrokerApp.Configuration;
using BrokerApp.Core;
using Microsoft.Extensions.Options;

namespace BrokerApp.Services;

// The "work job" from the lab: runs at a fixed interval,
// redelivers un-ACKed messages and moves exhausted ones to the DLQ.
// Replaces RetryLoopAsync from Part 1.
public sealed class RetryWorker : BackgroundService
{
    private readonly BrokerState state;
    private readonly BrokerOptions options;
    private readonly ILogger<RetryWorker> logger;

    public RetryWorker(
        BrokerState state,
        IOptions<BrokerOptions> options,
        ILogger<RetryWorker> logger)
    {
        this.state = state;
        this.options = options.Value;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Retry worker started: every {Interval} ms, ACK timeout {Timeout}s, max {Max} retries",
            options.RetryIntervalMs, options.AckTimeoutSeconds, options.MaxRetries);

        using var timer = new PeriodicTimer(
            TimeSpan.FromMilliseconds(options.RetryIntervalMs));

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    state.RunRetryCycle();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Retry cycle failed; worker keeps running");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Broker shutting down.
        }
    }
}