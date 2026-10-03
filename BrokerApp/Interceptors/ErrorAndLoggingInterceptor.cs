using System.Diagnostics;
using BrokerApp.Core;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace BrokerApp.Interceptors;

// One place for: logging every call with its deadline,
// translating domain errors into gRPC status codes,
// and making sure an unexpected exception never takes the broker down.
public sealed class ErrorAndLoggingInterceptor : Interceptor
{
    private readonly ILogger<ErrorAndLoggingInterceptor> logger;

    public ErrorAndLoggingInterceptor(ILogger<ErrorAndLoggingInterceptor> logger)
    {
        this.logger = logger;
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var watch = Stopwatch.StartNew();
        LogStart(context);

        try
        {
            return await continuation(request, context);
        }
        catch (Exception ex)
        {
            throw Translate(ex, context, watch);
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        var watch = Stopwatch.StartNew();
        LogStart(context);

        try
        {
            await continuation(request, responseStream, context);
        }
        catch (Exception ex)
        {
            throw Translate(ex, context, watch);
        }
    }

    private void LogStart(ServerCallContext context)
    {
        string deadline = context.Deadline == DateTime.MaxValue
            ? "none"
            : $"{(context.Deadline - DateTime.UtcNow).TotalMilliseconds:F0} ms left";

        logger.LogInformation(
            "-> {Method} from {Peer} | deadline: {Deadline}",
            context.Method, context.Peer, deadline);
    }

    private RpcException Translate(
        Exception ex,
        ServerCallContext context,
        Stopwatch watch)
    {
        RpcException result = ex switch
        {
            RpcException rpc => rpc,

            BrokerException broker =>
                new RpcException(new Status(broker.Code, broker.Message)),

            OperationCanceledException when context.CancellationToken.IsCancellationRequested =>
                new RpcException(new Status(
                    context.Deadline <= DateTime.UtcNow
                        ? StatusCode.DeadlineExceeded
                        : StatusCode.Cancelled,
                    "Call cancelled.")),

            _ => new RpcException(new Status(
                StatusCode.Internal,
                "Internal broker error. The broker is still running."))
        };

        if (result.StatusCode == StatusCode.Internal)
        {
            logger.LogError(ex, "<- {Method} failed unexpectedly", context.Method);
        }
        else
        {
            logger.LogWarning(
                "<- {Method} {Code}: {Detail} ({Ms} ms)",
                context.Method, result.StatusCode, result.Status.Detail,
                watch.ElapsedMilliseconds);
        }

        return result;
    }
}