using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using BrokerApp.Core;
using Grpc.Core;
using ReliableMessaging.Contracts.V1;

namespace BrokerApp.Validation;

// Protobuf guarantees the TYPES of the fields; the broker still has to
// check their VALUES. Invalid input => INVALID_ARGUMENT, never a crash.
public static class RequestValidator
{
    public const int MaxNameLength = 64;
    public const int MaxIdLength = 128;
    public const int MaxContentLength = 4096;

    private static readonly Regex NamePattern =
        new(@"^[\p{L}\p{N}_\-]+$", RegexOptions.Compiled);

    private static readonly Regex IdPattern =
        new(@"^[A-Za-z0-9_\-:.]+$", RegexOptions.Compiled);

    public static void Validate(PublishRequest request)
    {
        ValidatePublisherName(request.PublisherName);
        ValidateId(request.IdempotencyKey, "idempotency_key");
        ValidateContent(request.Content);
    }

    public static void ValidatePublisherName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            Fail("publisher_name is required.");
        }

        if (value.Length > MaxNameLength)
        {
            Fail($"publisher_name must be at most {MaxNameLength} characters.");
        }

        if (!NamePattern.IsMatch(value))
        {
            Fail("publisher_name may contain only letters, digits, '_' and '-'.");
        }
    }

    public static void ValidateId(string? value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            Fail($"{fieldName} is required.");
        }

        if (value.Length > MaxIdLength)
        {
            Fail($"{fieldName} must be at most {MaxIdLength} characters.");
        }

        if (!IdPattern.IsMatch(value))
        {
            Fail($"{fieldName} may contain only letters, digits and _ - : .");
        }
    }

    public static void ValidateContent(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            Fail("content must not be empty.");
        }

        if (value.Length > MaxContentLength)
        {
            Fail($"content must be at most {MaxContentLength} characters.");
        }

        if (value.Any(c => char.IsControl(c) && c != '\n' && c != '\r' && c != '\t'))
        {
            Fail("content contains invalid control characters.");
        }
    }

    [DoesNotReturn]
    private static void Fail(string message) =>
        throw new BrokerException(StatusCode.InvalidArgument, message);
}