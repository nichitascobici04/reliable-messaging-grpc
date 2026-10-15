using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using ReliableMessaging.Contracts.V1;
using SubscriberApp;

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("Cancellation requested...");
    cts.Cancel();
};

var channel = GrpcChannel.ForAddress("https://localhost:5001");

// generated client
var client = new BrokerService.BrokerServiceClient(channel);

// create subscriber (starts unsubscribed)
await using var subscriber = new SubscriberClient(client, subscriberId: "sub-001");

Console.WriteLine("Interactive subscriber console.");
Console.WriteLine("Commands:");
Console.WriteLine("  subscribe <publisherId>");
Console.WriteLine("  unsubscribe");
Console.WriteLine("  status");
Console.WriteLine("  exit");

while (!cts.IsCancellationRequested)
{
    Console.Write("> ");
    var line = Console.ReadLine();
    if (line is null) break;

    var parts = line.Trim().Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length == 0) continue;

    var cmd = parts[0].ToLowerInvariant();

    try
    {
        if (cmd == "subscribe")
        {
            if (parts.Length < 2)
            {
                Console.WriteLine("Usage: subscribe <publisherId>");
                continue;
            }

            var publisherId = parts[1].Trim();
            await subscriber.SubscribeAsync(publisherId, cts.Token);
            Console.WriteLine($"Requested subscription to '{publisherId}'.");
        }
        else if (cmd == "unsubscribe")
        {
            await subscriber.UnsubscribeAsync();
        }
        else if (cmd == "status")
        {
            var s = subscriber.CurrentlySubscribedTo;
            Console.WriteLine(s is null ? "Not subscribed" : $"Subscribed to: {s}");
        }
        else if (cmd == "exit")
        {
            break;
        }
        else
        {
            Console.WriteLine("Unknown command.");
        }
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error: {ex.Message}");
    }
}

Console.WriteLine("Shutting down...");
await subscriber.DisposeAsync();