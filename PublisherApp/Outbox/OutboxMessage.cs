using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace PublisherApp.Outbox;

public sealed class OutboxMessage
{
    public required string PublisherName { get; init; }

    public required string IdempotencyKey { get; init; }

    public required string Content { get; init; }

    public DateTime CreatedAtUtc { get; init; } = DateTime.UtcNow;
}