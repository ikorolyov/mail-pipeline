using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MailPipeline;

// Обрабатывает письма из q.mail. В RabbitMQ ничего не публикует: события пишет в outbox в той же транзакции.
public static class Processor
{
    private const string ConsumerName = "processor";

    public static async Task RunAsync()
    {
        await using var connection = await Rabbit.ConnectAsync(ConsumerName);
        await using var channel = await Rabbit.CreateChannelAsync(connection);
        await Rabbit.DeclareTopologyAsync(channel);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            // ProcessAsync не бросает исключений, поэтому каждая доставка получит ответ.
            var outcome = await ProcessAsync(ea);
            await Rabbit.RespondAsync(channel, ea.DeliveryTag, outcome);
        };

        await channel.BasicConsumeAsync(Rabbit.MailQueue, autoAck: false, consumer);

        Log.Write(ConsumerName, $"слушаю {Rabbit.MailQueue} (Ctrl+C — выход)");
        await Task.Delay(Timeout.Infinite);
    }

    private static async Task<OutcomeStatus> ProcessAsync(BasicDeliverEventArgs ea)
    {
        // x-delivery-count — число прошлых неудачных доставок (при первой его нет). nack его не увеличивает.
        var delivery = Rabbit.GetIntHeader(ea.BasicProperties, "x-delivery-count") + 1;

        MailDownloaded mail;
        try
        {
            mail = Json.FromBytes<MailDownloaded>(ea.Body);
        }
        catch (Exception ex)
        {
            // Тело не соответствует контракту — повтор не поможет.
            Log.Write(ConsumerName, $"{Log.Id(ea.BasicProperties.MessageId)} тело не разобрано: {ex.Message} -> reject, в {Rabbit.MailDeadLetterQueue}");
            return OutcomeStatus.RejectDead;
        }

        var prefix = $"{Log.Id(mail.MessageId)} {mail.Kind,-6}";
        try
        {
            if (mail.Kind == MailKind.Broken)
                throw new InvalidOperationException("не удалось разобрать письмо");

            await Task.Delay(Random.Shared.Next(100, 500)); // имитация разбора письма

            await using var db = await Db.DataSource.OpenConnectionAsync();
            await using var transaction = await db.BeginTransactionAsync();

            if (!await Db.TryAddToInboxAsync(db, transaction, ConsumerName, mail.MessageId))
            {
                // Коммитить нечего: транзакция откатится при Dispose.
                Log.Write(ConsumerName, $"{prefix} дубль -> ack");
                return OutcomeStatus.Ack;
            }

            Guid? eventId = null;
            if (mail.Kind == MailKind.Reply)
            {
                var reply = new ReplyReceived(Guid.NewGuid(), mail.MessageId, mail.AccountId, mail.From);
                await Db.AddToOutboxAsync(db, transaction, reply.EventId, reply);
                eventId = reply.EventId;
            }

            // Отметка в inbox и событие в outbox сохраняются атомарно.
            await transaction.CommitAsync();

            // ack только после коммита: упадём раньше — письмо придёт снова, и inbox отсечёт его как дубль.
            Log.Write(ConsumerName, $"{prefix} Account={mail.AccountId} -> ack" +
                (eventId is { } id ? $", событие {Log.Id(id)} в outbox" : ""));

            return OutcomeStatus.Ack;
        }
        catch (NpgsqlException ex) when (ex.IsTransient)
        {
            // nack не засчитывается в delivery-limit: временный сбой базы не отправит нормальные письма в DLQ.
            Log.Write(ConsumerName, $"{prefix} база недоступна: {ex.Message} -> nack через {Rabbit.TransientErrorDelay.TotalSeconds:0} с");
            await Task.Delay(Rabbit.TransientErrorDelay);
            return OutcomeStatus.NackRequeue;
        }
        catch (Exception ex)
        {
            // reject с requeue засчитывается в delivery-limit; после последней доставки письмо уйдёт в q.mail.dlq.
            Log.Write(ConsumerName, $"{prefix} доставка {delivery}/{Rabbit.MailDeliveryLimit + 1}: {ex.Message} -> reject requeue");
            return OutcomeStatus.RejectRequeue;
        }
    }
}
