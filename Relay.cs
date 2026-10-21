using System.Text;
using Npgsql;
using RabbitMQ.Client;

namespace MailPipeline;

// Outbox relay: раз в секунду забирает неотправленные события из outbox и публикует их в x.events.
public static class Relay
{
    private const string ClientName = "relay";
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SlowPublishDelay = TimeSpan.FromSeconds(1);

    public static async Task RunAsync(string[] args)
    {
        // --slow: пачки по 5 и пауза после каждой публикации — чтобы успеть убить процесс посреди пачки
        // и увидеть, как два relay делят строки.
        var slow = args.Contains("--slow");
        var batchSize = slow ? 5 : 100;

        await using var connection = await Rabbit.ConnectAsync(ClientName);
        await using var channel = await Rabbit.CreateChannelAsync(connection, confirms: true);
        await Rabbit.DeclareTopologyAsync(channel);

        Log.Write(ClientName, $"опрашиваю outbox раз в {PollInterval.TotalSeconds:0} с{(slow ? ", медленный режим" : "")} (Ctrl+C — выход)");

        while (true)
        {
            try
            {
                if (await PublishBatchAsync(channel, batchSize, slow) == 0)
                    await Task.Delay(PollInterval);
            }
            catch (Exception ex)
            {
                // Транзакция откатилась, события остались в outbox. Уже ушедшие из этой пачки
                // будут опубликованы повторно — подписчики отсекут их по inbox.
                Log.Write(ClientName, $"ошибка: {ex.Message} -> повтор через {Rabbit.TransientErrorDelay.TotalSeconds:0} с");
                await Task.Delay(Rabbit.TransientErrorDelay);
            }
        }
    }

    private static async Task<int> PublishBatchAsync(IChannel channel, int batchSize, bool slow)
    {
        await using var db = await Db.DataSource.OpenConnectionAsync();
        await using var transaction = await db.BeginTransactionAsync();

        // FOR UPDATE SKIP LOCKED: строки, взятые другим relay, пропускаем — одно событие не опубликуют двое.
        var rows = await Db.GetFromOutboxWithLockAsync(db, transaction, batchSize);

        if (rows.Count == 0)
            return 0;

        foreach (var row in rows)
        {
            // Routing key = тип события: fanout его игнорирует, но при переходе на topic Relay менять не придётся.
            await channel.BasicPublishAsync(Rabbit.EventsExchange, routingKey: row.Type, mandatory: true,
                basicProperties: Rabbit.GetProps(row.MessageId, row.Type), body: Encoding.UTF8.GetBytes(row.Payload));

            Log.Write(ClientName, $"{Log.Id(row.MessageId)} {row.Type} -> {Rabbit.EventsExchange}");

            if (slow)
                await Task.Delay(SlowPublishDelay);
        }

        // Упадём здесь — события уже в брокере, но sent_at не проставлен, и их опубликуют повторно.
        // Outbox гарантирует "как минимум один раз", поэтому потребителям событий нужен inbox.
        await Db.UpdateOutboxSentAtAsync(db, transaction, rows.Select(x => x.Id).ToArray());

        await transaction.CommitAsync();
        Log.Write(ClientName, $"пачка отмечена отправленной, событий: {rows.Count}");

        return rows.Count;
    }
}
