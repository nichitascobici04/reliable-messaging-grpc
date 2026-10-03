using Grpc.Core;

namespace BrokerApp.Core;

// Domain error carrying the gRPC status code it maps to.
// Translated to RpcException in ErrorAndLoggingInterceptor.
public sealed class BrokerException : Exception
{
    public BrokerException(StatusCode code, string message)
        : base(message)
    {
        Code = code;
    }

    public StatusCode Code { get; }
}