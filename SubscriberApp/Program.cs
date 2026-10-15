using Grpc.Net.Client;
using ReliableMessaging.Contracts.V1;
using SubscriberApp;

// The broker listens on plain HTTP/2 (h2c) on port 5000 - see BrokerApp/Program.cs.
// Override with the BROKER_ADDRESS environment variable if needed.
string brokerAddress =
    Environment.GetEnvironmentVariable("BROKER_ADDRESS") ?? "http://localhost:5000";

Console.WriteLine("=================================");
Console.WriteLine("       SUBSCRIBER APP (gRPC)");
Console.WriteLine("=================================");

// Every subscriber process needs its OWN id (two processes with the same id
// would replace each other's stream). Reusing an id after a crash resumes
// the broker session and the local dedup store.
// Can be passed as an argument: dotnet run --project SubscriberApp -- sub-1
string? subscriberId = args.Length > 0 ? args[0] : null;

if (string.IsNullOrWhiteSpace(subscriberId))
{
    string generated = $"sub-{Guid.NewGuid().ToString("N")[..6]}";
    Console.Write($"Enter Subscriber ID (Enter = {generated}): ");
    subscriberId = Console.ReadLine()?.Trim();

    if (string.IsNullOrWhiteSpace(subscriberId))
    {
        subscriberId = generated;
    }
}

using var cts = new CancellationTokenSource();
using var channel = GrpcChannel.ForAddress(brokerAddress);

var client = new BrokerService.BrokerServiceClient(channel);
var subscriber = new SubscriberClient(client, subscriberId);

Console.CancelKeyPress += (_, e) =>
{
    // Cancellation: the open stream is cancelled, the broker detaches the
    // session and keeps un-ACKed messages for when this id reconnects.
    Console.WriteLine("\nStopping subscriber...");
    cts.Cancel();
    subscriber.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(2));
};

Console.WriteLine($"Subscriber ID: {subscriberId}");
Console.WriteLine($"Broker: {brokerAddress}");
PrintMenu();

while (!cts.IsCancellationRequested)
{
    Console.Write("> ");
    string? line = Console.ReadLine();

    if (line is null || cts.IsCancellationRequested)
    {
        break;
    }

    string[] parts = line.Trim().Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);

    if (parts.Length == 0)
    {
        continue;
    }

    try
    {
        switch (parts[0].ToLowerInvariant())
        {
            case "subscribe":
            case "1":
                string? publisherName = parts.Length > 1 ? parts[1].Trim() : null;

                if (string.IsNullOrWhiteSpace(publisherName))
                {
                    Console.Write("Enter Publisher Name: ");
                    publisherName = Console.ReadLine()?.Trim();
                }

                if (string.IsNullOrWhiteSpace(publisherName))
                {
                    Console.WriteLine("Publisher name cannot be empty.");
                    break;
                }

                await subscriber.SubscribeAsync(publisherName, cts.Token);
                Console.WriteLine($"Subscription request sent for '{publisherName}'.");
                break;

            case "unsubscribe":
            case "2":
                await subscriber.UnsubscribeAsync(cts.Token);
                break;

            case "dlq":
            case "3":
                await subscriber.ShowDeadLettersAsync(cts.Token);
                break;

            case "status":
            case "4":
                subscriber.PrintStatus();
                break;

            case "exit":
            case "5":
                cts.Cancel();
                break;

            default:
                PrintMenu();
                break;
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

await subscriber.DisposeAsync();
Console.WriteLine("Subscriber stopped.");

static void PrintMenu()
{
    Console.WriteLine();
    Console.WriteLine("Commands:");
    Console.WriteLine("  1 | subscribe <publisher>   subscribe (full history is replayed)");
    Console.WriteLine("  2 | unsubscribe");
    Console.WriteLine("  3 | dlq                     view the Dead Letter Queue");
    Console.WriteLine("  4 | status");
    Console.WriteLine("  5 | exit");
    Console.WriteLine();
}
