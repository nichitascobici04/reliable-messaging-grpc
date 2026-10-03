using BrokerApp.Configuration;
using BrokerApp.Core;
using BrokerApp.Faults;
using BrokerApp.Interceptors;
using BrokerApp.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

builder.Services.Configure<BrokerOptions>(
    builder.Configuration.GetSection(BrokerOptions.SectionName));

int port = builder.Configuration.GetValue<int>(
    $"{BrokerOptions.SectionName}:Port", 5000);

// gRPC runs over HTTP/2. Without TLS (local lab) the endpoint
// must be HTTP/2-only ("h2c"), otherwise clients cannot negotiate it.
builder.WebHost.ConfigureKestrel(kestrel =>
    kestrel.ListenAnyIP(port, listen =>
        listen.Protocols = HttpProtocols.Http2));

// Broker state is a singleton: one in-memory (transient) storage
// shared by all concurrent gRPC calls.
builder.Services.AddSingleton<BrokerState>();
builder.Services.AddSingleton<FaultInjector>();
builder.Services.AddHostedService<RetryWorker>();

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<ErrorAndLoggingInterceptor>();
    options.MaxReceiveMessageSize = 64 * 1024; // reject oversized requests early
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

// Reflection lets grpcurl / Postman discover the service without the .proto file.
builder.Services.AddGrpcReflection();

var app = builder.Build();

app.MapGrpcService<BrokerGrpcService>();
app.MapGrpcReflectionService();

Console.WriteLine("=================================");
Console.WriteLine("        BROKER APP (gRPC)");
Console.WriteLine("=================================");
Console.WriteLine($"Listening on http://localhost:{port} (HTTP/2, plaintext)");
Console.WriteLine();

app.Run();