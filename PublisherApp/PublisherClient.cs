using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using PublisherApp.Outbox;
using PublisherApp.Resilience;
using ReliableMessaging.Contracts.V1;

namespace PublisherApp;

public sealed class PublisherClient
{
    private readonly BrokerService.BrokerServiceClient _client;

    public PublisherClient(GrpcChannel channel)
    {
        _client = new BrokerService.BrokerServiceClient(channel);
    }

    public async Task<PublishReply> PublishAsync(
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

        return await _client.PublishAsync(
            request,
            deadline: DateTime.UtcNow.AddSeconds(5),
            cancellationToken: cancellationToken
        ).ResponseAsync;
    }

    public async Task<ServerInfo> GetServerInfoAsync(
        CancellationToken cancellationToken = default)
    {
        return await _client.GetServerInfoAsync(
            new GetServerInfoRequest(),
            deadline: DateTime.UtcNow.AddSeconds(5),
            cancellationToken: cancellationToken
        ).ResponseAsync;
    }

    public async Task<RegisterPublisherReply> RegisterPublisherAsync(
        string publisherName,
        CancellationToken cancellationToken = default)
    {
        var request = new RegisterPublisherRequest
        {
            PublisherName = publisherName
        };

        return await _client.RegisterPublisherAsync(
            request,
            deadline: DateTime.UtcNow.AddSeconds(5),
            cancellationToken: cancellationToken
        ).ResponseAsync;
    }
}