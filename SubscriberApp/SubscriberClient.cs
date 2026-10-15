using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using ReliableMessaging.Contracts.V1;
using SubscriberApp.Deduplication;
using SubscriberApp.Ordering;

namespace SubscriberApp;

public sealed class SubscriberClient : IAsyncDisposable
{
    private readonly BrokerService.BrokerServiceClient _client;
    private readonly string _subscriberId;

    private readonly ProcessedMessageStore _processed = new();
    private readonly MessageOrderTracker _ordering = new();

    // Stream management
    private CancellationTokenSource? _streamCts;
    private Task? _listenTask;
    private string? _subscribedPublisherId;

    public SubscriberClient(
        BrokerService.BrokerServiceClient client,
        string subscriberId)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _subscriberId = subscriberId ?? throw new ArgumentNullException(nameof(subscriberId));
    }

    // Start a subscription to publisherId. If already subscribed, it will be replaced.
    public Task SubscribeAsync(string publisherId, CancellationToken appCancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(publisherId))
            throw new ArgumentException("publisherId required", nameof(publisherId));

        CancelActiveStream();

        _streamCts = CancellationTokenSource.CreateLinkedTokenSource(appCancellationToken);
        var token = _streamCts.Token;

        _subscribedPublisherId = publisherId;

        var request = new SubscribeRequest
        {
            SubscriberId = _subscriberId,
            // The proto uses PublisherName; populate it with the provided publisherId value.
            PublisherName = publisherId,
            SkipHistory = false
        };

        _listenTask = Task.Run(async () =>
        {
            using var call = _client.Subscribe(request, cancellationToken: token);

            try
            {
                await foreach (var ev in call.ResponseStream.ReadAllAsync(token))
                {
                    switch (ev.EventCase)
                    {
                        case SubscriberEvent.EventOneofCase.Started:
                        {
                            var started = ev.Started;
                            Console.WriteLine(
                                $"[Started] Resumed={started.Resumed}, Replayed={started.ReplayedMessages}, Redelivered={started.RedeliveredMessages}");
                            break;
                        }

                        case SubscriberEvent.EventOneofCase.Delivery:
                        {
                            var delivery = ev.Delivery;
                            try
                            {
                                await HandleDeliveryAsync(delivery, token);
                            }
                            catch (OperationCanceledException) when (token.IsCancellationRequested)
                            {
                                return;
                            }
                            break;
                        }

                        case SubscriberEvent.EventOneofCase.Abandoned:
                        {
                            var abandoned = ev.Abandoned;
                            Console.WriteLine(
                                $"[DLQ] Delivery {abandoned.DeliveryId} seq #{abandoned.SequenceNumber} reason: {abandoned.Reason}");
                            break;
                        }

                        case SubscriberEvent.EventOneofCase.None:
                        default:
                            break;
                    }
                }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Aborted)
            {
                Console.WriteLine("[Info] Stream replaced by newer Subscribe call.");
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
            {
                Console.WriteLine($"[Warning] Publisher with id '{publisherId}' not found: {ex.Status.Detail ?? ex.Message}");
            }
            catch (RpcException ex)
            {
                Console.WriteLine($"[Error] Subscribe stream ended: {ex.StatusCode} - {ex.Status.Detail ?? ex.Message}");
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // expected
            }
        }, CancellationToken.None);

        return Task.CompletedTask;
    }

    // Ask broker to unsubscribe and cancel local stream.
    public async Task UnsubscribeAsync()
    {
        CancelActiveStream();

        try
        {
            var req = new UnsubscribeRequest { SubscriberId = _subscriberId };
            var reply = await _client.UnsubscribeAsync(req).ResponseAsync;
            Console.WriteLine($"[Unsubscribe] WasSubscribed={reply.WasSubscribed}");
            _subscribedPublisherId = null;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"[Error] Unsubscribe RPC failed: {ex.StatusCode} - {ex.Status.Detail ?? ex.Message}");
        }
    }

    public string? CurrentlySubscribedTo => _subscribedPublisherId;

    public async ValueTask DisposeAsync()
    {
        CancelActiveStream();
        if (_listenTask is not null)
        {
            try { await _listenTask; } catch { /* ignore */ }
        }
    }

    private void CancelActiveStream()
    {
        if (_streamCts is not null)
        {
            try
            {
                if (!_streamCts.IsCancellationRequested)
                    _streamCts.Cancel();
            }
            catch { /* ignore */ }
            finally
            {
                _streamCts.Dispose();
                _streamCts = null;
            }
        }
    }

    private async Task HandleDeliveryAsync(Delivery delivery, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"[Delivery] Publisher={delivery.PublisherName} Seq={delivery.SequenceNumber} Id={delivery.DeliveryId}");

        if (_processed.HasProcessed(delivery.DeliveryId))
        {
            Console.WriteLine($"[Duplicate] {delivery.DeliveryId} -> ACK");
            await SendAckAsync(delivery.DeliveryId, cancellationToken);
            return;
        }

        if (!_ordering.IsExpected(delivery.PublisherName, delivery.SequenceNumber))
        {
            Console.WriteLine($"[OutOfOrder] Seq={delivery.SequenceNumber} Buffering");
            _ordering.Buffer(delivery.PublisherName, delivery);
            return;
        }

        await ProcessAndAckAsync(delivery, cancellationToken);

        var ready = _ordering.TakeReadyMessages(delivery.PublisherName);
        foreach (var d in ready)
            await ProcessAndAckAsync(d, cancellationToken);
    }

    private async Task ProcessAndAckAsync(Delivery delivery, CancellationToken cancellationToken)
    {
        try
        {
            Console.WriteLine($"[Process] {delivery.Content}");
            await Task.Delay(100, cancellationToken);

            _processed.TryMarkProcessed(delivery.DeliveryId);

            await SendAckAsync(delivery.DeliveryId, cancellationToken);

            Console.WriteLine($"[ACK] {delivery.DeliveryId}");
        }
        catch
        {
            await SendNackAsync(delivery.DeliveryId, cancellationToken);
            throw;
        }
    }

    private async Task SendAckAsync(string deliveryId, CancellationToken cancellationToken)
    {
        var req = new AckRequest { SubscriberId = _subscriberId, DeliveryId = deliveryId };
        try
        {
            await _client.AckAsync(req, deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellationToken).ResponseAsync;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"[Error] ACK failed: {ex.StatusCode} - {ex.Status.Detail ?? ex.Message}");
            throw;
        }
    }

    private async Task SendNackAsync(string deliveryId, CancellationToken cancellationToken)
    {
        var req = new NackRequest { SubscriberId = _subscriberId, DeliveryId = deliveryId, Reason = "Processing failed" };
        try
        {
            await _client.NackAsync(req, deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellationToken).ResponseAsync;
        }
        catch (RpcException)
        {
            // best-effort
        }
    }
}