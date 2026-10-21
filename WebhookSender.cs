using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace MailPipeline;

// Отправляет "вебхук" на каждое событие ReplyReceived; HTTP имитируется, половина вызовов падает.
// Неудача -> q.webhook.retry (10 с) -> снова q.webhook. После 3 попыток -> q.webhook.failed.
public static class WebhookSender
{
    private const string ConsumerName = "webhook";
    private const string AttemptHeader = "attempt";
    private const int MaxAttempts = 3;

    public static async Task RunAsync()
    {
        await using var connection = await Rabbit.ConnectAsync(ConsumerName);
        await using var channel = await Rabbit.CreateChannelAsync(connection);
        await using var publishChannel = await Rabbit.CreateChannelAsync(connection, confirms: true);
        await Rabbit.DeclareTopologyAsync(channel);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 5, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            // HandleAsync не бросает исключений, поэтому каждая доставка получит ответ.
            var outcome = await HandleAsync(publishChannel, ea);
            await Rabbit.RespondAsync(channel, ea.DeliveryTag, outcome);
        };

        await channel.BasicConsumeAsync(Rabbit.WebhookQueue, autoAck: false, consumer);

        Log.Write(ConsumerName, $"слушаю {Rabbit.WebhookQueue} (Ctrl+C — выход)");
        await Task.Delay(Timeout.Infinite);
    }

    private static async Task<OutcomeStatus> HandleAsync(IChannel publishChannel, BasicDeliverEventArgs ea)
    {
        // x.events — fanout, сюда приходят события всех типов.
        if (ea.BasicProperties.Type != nameof(ReplyReceived))
        {
            Log.Write(ConsumerName, $"{Log.Id(ea.BasicProperties.MessageId)} {ea.BasicProperties.Type ?? "(без типа)"} вебхуку не нужен -> ack");
            return OutcomeStatus.Ack;
        }

        ReplyReceived reply;
        try
        {
            reply = Json.FromBytes<ReplyReceived>(ea.Body);
        }
        catch (Exception ex)
        {
            // Событие не соответствует контракту — повтор не поможет.
            Log.Write(ConsumerName, $"{Log.Id(ea.BasicProperties.MessageId)} тело не разобрано: {ex.Message} -> reject, в {Rabbit.WebhookFailedQueue}");
            return OutcomeStatus.RejectDead;
        }

        var attempt = Rabbit.GetIntHeader(ea.BasicProperties, AttemptHeader, defaultValue: 1);
        var prefix = $"{Log.Id(reply.EventId)} попытка {attempt}/{MaxAttempts}";

        try
        {
            await using var db = await Db.DataSource.OpenConnectionAsync();
            await using var transaction = await db.BeginTransactionAsync();

            // Отметка в inbox до HTTP и в той же транзакции: копия события у второго экземпляра webhook
            // дождётся нашего коммита и увидит дубль.
            if (!await Db.TryAddToInboxAsync(db, transaction, ConsumerName, reply.EventId))
            {
                Log.Write(ConsumerName, $"{Log.Id(reply.EventId)} дубль, вебхук уже отправлен -> ack");
                return OutcomeStatus.Ack;
            }

            if (await FakeHttpPostAsync())
            {
                // Упадём до коммита — вебхук уйдёт повторно: HTTP не участвует в транзакции БД.
                // Повторы отсекает получатель по EventId (ключ идемпотентности).
                await transaction.CommitAsync();
                Log.Write(ConsumerName, $"{prefix}: отправлен -> ack");
                return OutcomeStatus.Ack;
            }

            await transaction.RollbackAsync();

            if (attempt < MaxAttempts)
            {
                await RepublishAsync(publishChannel, Rabbit.WebhookRetryQueue, ea.Body, reply.EventId, attempt + 1);
                Log.Write(ConsumerName, $"{prefix}: ошибка HTTP -> {Rabbit.WebhookRetryQueue} ({Rabbit.WebhookRetryDelay.TotalSeconds:0} с), ack");
            }
            else
            {
                await RepublishAsync(publishChannel, Rabbit.WebhookFailedQueue, ea.Body, reply.EventId, attempt);
                Log.Write(ConsumerName, $"{prefix}: ошибка HTTP -> {Rabbit.WebhookFailedQueue}, ack");
            }

            // Оригинал подтверждаем: копия уже в retry- или failed-очереди (publisher confirms).
            return OutcomeStatus.Ack;
        }
        catch (Exception ex) when (ex is NpgsqlException { IsTransient: true } or OperationInterruptedException or PublishException)
        {
            // Событие не виновато: nack не засчитывается в delivery-limit.
            Log.Write(ConsumerName, $"{Log.Id(reply.EventId)} база или брокер недоступны: {ex.Message} -> nack через {Rabbit.TransientErrorDelay.TotalSeconds:0} с");
            await Task.Delay(Rabbit.TransientErrorDelay);
            return OutcomeStatus.NackRequeue;
        }
        catch (Exception ex)
        {
            // reject с requeue засчитывается в delivery-limit (по умолчанию 20), после него — в q.webhook.failed.
            Log.Write(ConsumerName, $"{prefix}: {ex.Message} -> reject requeue");
            return OutcomeStatus.RejectRequeue;
        }
    }

    // Default exchange: routing key = имя очереди. Заголовки — с нуля, только номер попытки:
    // служебные заголовки брокера (например, x-death с прошлого круга) переносить незачем.
    private static async Task RepublishAsync(IChannel channel, string queue, ReadOnlyMemory<byte> body, Guid eventId, int attempt)
    {
        var props = Rabbit.GetProps(eventId, nameof(ReplyReceived));
        props.Headers = new Dictionary<string, object?> { [AttemptHeader] = attempt };

        await channel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: true, basicProperties: props, body: body);
    }

    private static async Task<bool> FakeHttpPostAsync()
    {
        await Task.Delay(Random.Shared.Next(100, 400));
        return Random.Shared.NextDouble() >= 0.5;
    }
}
