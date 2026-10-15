using Grpc.Core;
using Grpc.Net.Client;
using PublisherApp;
using PublisherApp.Outbox;
using PublisherApp.Resilience;
using ReliableMessaging.Contracts.V1;

// The broker listens on plain HTTP/2 (h2c) on port 5000 - see BrokerApp/Program.cs.
// Override with the BROKER_ADDRESS environment variable if needed.
string brokerAddress =
    Environment.GetEnvironmentVariable("BROKER_ADDRESS") ?? "http://localhost:5000";

Console.WriteLine("=================================");
Console.WriteLine("        PUBLISHER APP (gRPC)");
Console.WriteLine("=================================");

// Publisher name = topic (subject) that subscribers subscribe to.
// Can be passed as an argument: dotnet run --project PublisherApp -- VOLEYBOL
string? publisherName = args.Length > 0 ? args[0] : null;

if (string.IsNullOrWhiteSpace(publisherName))
{
    Console.Write("Enter Publisher Name: ");
    publisherName = Console.ReadLine()?.Trim();
}

if (string.IsNullOrWhiteSpace(publisherName))
{
    Console.WriteLine("Publisher name cannot be empty.");
    return;
}

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    // Cancels every in-flight gRPC call and the outbox worker.
    cts.Cancel();
    Console.WriteLine("\nStopping publisher...");
};

using var channel = GrpcChannel.ForAddress(brokerAddress);
var client = new BrokerService.BrokerServiceClient(channel);
var publisher = new PublisherClient(client);
var outbox = new FileOutbox($"outbox-{publisherName}.json");

// Register first, so subscribers can subscribe before the first message.
try
{
    var info = await publisher.GetServerInfoAsync(cts.Token);
    var reg = await publisher.RegisterPublisherAsync(publisherName, cts.Token);

    Console.WriteLine(
        $"Connected to broker {brokerAddress} (API {info.ApiVersion}, " +
        $"ACK timeout {info.AckTimeoutSeconds}s, max retries {info.MaxRetries}).");

    Console.WriteLine(
        $"Publisher '{reg.PublisherName}' " +
        (reg.AlreadyRegistered
            ? $"was already registered ({reg.StoredMessages} stored message(s))."
            : "registered."));
}
catch (RpcException ex) when (RetryPolicy.IsTransient(ex))
{
    Console.WriteLine(
        $"Broker not reachable at {brokerAddress} ({ex.StatusCode}). " +
        "Messages will be kept in the outbox and sent when it comes back.");
}
catch (RpcException ex)
{
    Console.WriteLine($"Registration rejected: {ex.StatusCode} - {ex.Status.Detail}");
    return;
}

var worker = new OutboxWorker(publisher, outbox, publisherName);
Task workerTask = worker.RunAsync(cts.Token);

Console.WriteLine();
Console.WriteLine("Type a message and press Enter ('exit' to quit).");
Console.WriteLine("  include #lose-response -> broker stores it, then 'loses' the reply");
Console.WriteLine("  include FAIL           -> the subscriber fails to process it (-> DLQ)");
Console.WriteLine();

while (!cts.IsCancellationRequested)
{
    Console.Write("Message: ");
    string? content = Console.ReadLine();

    if (content is null || cts.IsCancellationRequested)
    {
        break;
    }

    if (content.Trim().Equals("exit", StringComparison.OrdinalIgnoreCase))
    {
        break;
    }

    if (string.IsNullOrWhiteSpace(content))
    {
        Console.WriteLine("Message cannot be empty.");
        continue;
    }

    // ONE key per logical message, reused by every retry of that message.
    var message = new OutboxMessage
    {
        PublisherName = publisherName,
        IdempotencyKey = Guid.NewGuid().ToString("N"),
        Content = content
    };

    // Outbox first: if the app crashes while waiting for the reply,
    // the message is still on disk and will be resent with the same key.
    await outbox.AddAsync(message, cts.Token);

    try
    {
        PublishReply reply = await publisher.PublishWithRetryAsync(message, cts.Token);

        await outbox.RemoveAsync(message.IdempotencyKey, cts.Token);

        if (reply.Duplicate)
        {
            Console.WriteLine(
                $"  Broker answered DUPLICATE: the first attempt had already been stored " +
                $"(message {reply.MessageId}, offset {reply.Offset}). Nothing was stored twice.");
        }
        else
        {
            Console.WriteLine(
                $"  Stored by broker: message {reply.MessageId}, offset {reply.Offset}, " +
                $"queued for {reply.QueuedForSubscribers} subscriber(s).");
        }
    }
    catch (RpcException ex) when (RetryPolicy.IsTransient(ex))
    {
        Console.WriteLine(
            $"  Outcome UNKNOWN after retries ({ex.StatusCode}). Kept in outbox; " +
            "it will be resent with the same key, so it cannot be duplicated.");
    }
    catch (RpcException ex)
    {
        await outbox.RemoveAsync(message.IdempotencyKey, CancellationToken.None);
        Console.WriteLine($"  Rejected by broker: {ex.StatusCode} - {ex.Status.Detail}");
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        break;
    }

    Console.WriteLine();
}

cts.Cancel();

try
{
    await workerTask;
}
catch (OperationCanceledException)
{
}

Console.WriteLine("Publisher stopped.");
