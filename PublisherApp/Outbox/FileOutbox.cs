using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;
using System.Threading;

namespace PublisherApp.Outbox;

public sealed class FileOutbox
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public FileOutbox(string filePath = "outbox.json")
    {
        _filePath = filePath;
    }

    public async Task AddAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            var messages = await LoadInternalAsync(cancellationToken);

            messages.RemoveAll(x =>
                x.IdempotencyKey == message.IdempotencyKey);

            messages.Add(message);

            await SaveInternalAsync(messages, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<List<OutboxMessage>> GetAllAsync(
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            return await LoadInternalAsync(cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task RemoveAsync(
        string idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            var messages = await LoadInternalAsync(cancellationToken);

            messages.RemoveAll(x =>
                x.IdempotencyKey == idempotencyKey);

            await SaveInternalAsync(messages, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<List<OutboxMessage>> LoadInternalAsync(
        CancellationToken cancellationToken)
    {
        if (!File.Exists(_filePath))
            return new List<OutboxMessage>();

        await using var stream = File.OpenRead(_filePath);

        return await JsonSerializer.DeserializeAsync<List<OutboxMessage>>(
            stream,
            cancellationToken: cancellationToken) ?? new List<OutboxMessage>();
    }

    private async Task SaveInternalAsync(
        List<OutboxMessage> messages,
        CancellationToken cancellationToken)
    {
        await using var stream = File.Create(_filePath);

        await JsonSerializer.SerializeAsync(
            stream,
            messages,
            new JsonSerializerOptions
            {
                WriteIndented = true
            },
            cancellationToken);
    }
}
