using Grpc.Net.Client;
using PublisherApp;
using PublisherApp.Outbox;
using System.Diagnostics.Contracts;

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var channel = GrpcChannel.ForAddress(
    "https://localhost:5001");

var outbox = new FileOutbox();

var client = new Contracts.MessageBroker.MessageBrokerClient(channel);

var publisher = new PublisherClient(
    channel,
    publisherId: "pub-001",
    outbox);

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

    var success = await publisher.PublishAsync(
        topic: "sports",
        payload,
        cts.Token);

    Console.WriteLine(
        success
            ? "Message accepted by broker."
            : "Message placed in outbox.");
}