using System;
using System.Threading;
using System.Threading.Tasks;
using ReliableMessaging.Contracts.V1;
using Grpc.Core;

namespace PublisherApp.Outbox;

public sealed class OutboxWorker
{
    private readonly BrokerService.BrokerServiceClient _client;
    private readonly FileOutbox _outbox;
    private readonly string _publisherId;

    public OutboxWorker(
        BrokerService.BrokerServiceClient client,
        FileOutbox outbox,
        string publisherId)
    {
        _client = client;
        _outbox = outbox;
        _publisherId = publisherId;
    }

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Ensure server is reachable (await ResponseAsync)
                await _client.GetServerInfoAsync(
                    new GetServerInfoRequest(),
                    deadline: DateTime.UtcNow.AddSeconds(5),
                    cancellationToken: cancellationToken).ResponseAsync;

                await FlushOutboxAsync(cancellationToken);
            }
            catch (RpcException)
            {
                // Broker still unavailable.
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // cancellation requested -> exit loop
                break;
            }

            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(5),
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task FlushOutboxAsync(
        CancellationToken cancellationToken)
    {
        var messages =
            await _outbox.GetAllAsync(cancellationToken);

        foreach (var message in messages)
        {
            var request = new PublishRequest
            {
                PublisherName = message.PublisherName,
                IdempotencyKey = message.IdempotencyKey,
                Content = message.Content
            };

            try
            {
                // Call RPC and await the response
                var response = await _client.PublishAsync(
                    request,
                    deadline: DateTime.UtcNow.AddSeconds(5)).ResponseAsync;

                // If we received a reply, treat it as delivered (broker accepted)
                await _outbox.RemoveAsync(
                    message.IdempotencyKey,
                    cancellationToken);

                Console.WriteLine(
                    $"Outbox message {message.IdempotencyKey} delivered.");
            }
            catch (RpcException ex)
                when (ex.StatusCode is
                    StatusCode.Unavailable or
                    StatusCode.DeadlineExceeded)
            {
                // Keep it in outbox.
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}