using System;
using System.Threading;
using Grpc.Net.Client;
using PublisherApp;
using PublisherApp.Outbox;
using ReliableMessaging.Contracts.V1;

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var channel = GrpcChannel.ForAddress(
    "https://localhost:5001");

var outbox = new FileOutbox();

var client = new BrokerService.BrokerServiceClient(channel);

var publisher = new PublisherClient(channel);

var worker = new OutboxWorker(
    client,
    outbox,
    "pub-001");

_ = worker.RunAsync(cts.Token);

Console.WriteLine("Publisher started.");
Console.WriteLine("Publisher ID: pub-001");

while (!cts.Token.IsCancellationRequested)
{
    Console.Write("Message: ");

    var payload = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(payload))
        continue;

    // Build an OutboxMessage (PublisherClient.PublishAsync expects OutboxMessage)
    var message = new OutboxMessage
    {
        PublisherName = "pub-001",
        IdempotencyKey = Guid.NewGuid().ToString("N"),
        Content = payload
    };

    try
    {
        // Try immediate publish; if it fails we persist to outbox.
        await publisher.PublishAsync(message, cts.Token);

        Console.WriteLine("Message accepted by broker.");
    }
    catch (Exception)
    {
        // Broker unreachable or other error -> persist to outbox for retry
        await outbox.AddAsync(message, cts.Token);

        Console.WriteLine("Message placed in outbox.");
    }
}