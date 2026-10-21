using Npgsql;

namespace MailPipeline;

public static class Db
{
    public static readonly NpgsqlDataSource DataSource =
        NpgsqlDataSource.Create("Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=mailpipeline");

    // false — дубль. Отметка живёт в транзакции вызывающего; если ту же пару сейчас вставляет
    // другая транзакция, INSERT дождётся её завершения.
    public static async Task<bool> TryAddToInboxAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string consumer, Guid messageId)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO inbox (consumer, message_id) VALUES (@consumer, @messageId) ON CONFLICT DO NOTHING",
            connection, transaction);
        command.Parameters.AddWithValue("consumer", consumer);
        command.Parameters.AddWithValue("messageId", messageId);

        return await command.ExecuteNonQueryAsync() == 1;
    }

    public static async Task AddToOutboxAsync<T>(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid messageId, T @event)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO outbox (message_id, type, payload) VALUES (@messageId, @type, @payload::jsonb)",
            connection, transaction);
        command.Parameters.AddWithValue("messageId", messageId);
        command.Parameters.AddWithValue("type", typeof(T).Name);
        command.Parameters.AddWithValue("payload", Json.Serialize(@event));

        await command.ExecuteNonQueryAsync();
    }

    public static async Task<List<OutboxModel>> GetFromOutboxWithLockAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int limit)
    {
        var rows = new List<OutboxModel>();
        await using (var select = new NpgsqlCommand(
            "SELECT id, message_id, type, payload FROM outbox " +
            "WHERE sent_at IS NULL ORDER BY id LIMIT @limit FOR UPDATE SKIP LOCKED",
            connection, transaction))
        {
            select.Parameters.AddWithValue("limit", limit);
            await using var reader = await select.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                rows.Add(new (reader.GetInt64(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3)));
        }
        return rows;
    }

    public static async Task UpdateOutboxSentAtAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, long[] ids)
    {
        await using var update = new NpgsqlCommand("UPDATE outbox SET sent_at = now() WHERE id = ANY(@ids)", connection, transaction);
        update.Parameters.AddWithValue("ids", ids);
        await update.ExecuteNonQueryAsync();
    }
}
