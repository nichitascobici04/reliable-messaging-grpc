using System;
using System.Threading;
using ReliableMessaging.Contracts.V1;
using Grpc.Net.Client;
using SubscriberApp;

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;

    Console.WriteLine(
        "Cancellation requested...");

    cts.Cancel();
};

var channel = GrpcChannel.ForAddress(
    "https://localhost:5001");  

// Use the generated BrokerService client (matches the .proto service).
var client = new BrokerService.BrokerServiceClient(channel);

var subscriber = new SubscriberClient(
    client,
    subscriberId: "sub-001",
    publisherId: "pub-001");

Console.WriteLine("Subscriber started.");
Console.WriteLine("Subscriber ID: sub-001");
Console.WriteLine("Publisher: pub-001");
Console.WriteLine("Press Ctrl+C to stop.");

await subscriber.RunAsync(cts.Token);