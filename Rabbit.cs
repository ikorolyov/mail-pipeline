using RabbitMQ.Client;

namespace MailPipeline;

// Ответ потребителя брокеру: ровно один на каждую доставку.
public enum OutcomeStatus
{
    Ack,
    NackRequeue,   // Сообщение не виновато (например, недоступна база). В RabbitMQ 4.3+ не засчитывается в delivery-limit.
    RejectRequeue, // Обработка не удалась. Засчитывается в delivery-limit, после лимита — в DLX.
    RejectDead,    // Сообщение негодное, повтор не поможет: сразу в DLX.
}

public static class Rabbit
{
    public const string MailExchange = "x.mail";
    public const string MailDeadLetterExchange = "x.mail.dlx";
    public const string EventsExchange = "x.events";

    public const string MailQueue = "q.mail";
    public const string MailDeadLetterQueue = "q.mail.dlq";
    public const string WebhookQueue = "q.webhook";
    public const string WebhookRetryQueue = "q.webhook.retry";
    public const string WebhookFailedQueue = "q.webhook.failed";
    public const string AuditQueue = "q.audit";

    public const string MailRoutingKey = "mail";

    // Сколько повторных неудачных доставок допускает q.mail: письмо придёт до MailDeliveryLimit + 1 раз.
    public const int MailDeliveryLimit = 3;

    public static readonly TimeSpan WebhookRetryDelay = TimeSpan.FromSeconds(10);

    // Пауза перед nack, чтобы не крутить сообщения вхолостую, пока база или брокер недоступны.
    public static readonly TimeSpan TransientErrorDelay = TimeSpan.FromSeconds(2);

    // Автовосстановление включено по умолчанию: после обрыва клиент сам переподключится,
    // заново объявит топологию и подпишет потребителей.
    public static async Task<IConnection> ConnectAsync(string clientName)
    {
        var connection = await new ConnectionFactory { HostName = "localhost", ClientProvidedName = clientName }.CreateConnectionAsync();

        connection.ConnectionShutdownAsync += (_, e) =>
        {
            // Application — соединение закрыли мы сами при выходе, это не обрыв.
            if (e.Initiator != ShutdownInitiator.Application)
                Log.Write(clientName, $"соединение потеряно: {e.ReplyText}");
            return Task.CompletedTask;
        };
        connection.RecoverySucceededAsync += (_, _) =>
        {
            Log.Write(clientName, "соединение восстановлено");
            return Task.CompletedTask;
        };

        return connection;
    }

    // confirms = true: BasicPublishAsync ждёт подтверждения брокера,
    // а при отказе (nack или сообщение некуда доставить) бросает исключение.
    public static async Task<IChannel> CreateChannelAsync(IConnection connection, bool confirms = false)
    {
        var channel = await connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: confirms,
            publisherConfirmationTrackingEnabled: confirms));

        // Исключение из обработчика сообщения клиент не пробрасывает, а сообщает только через это событие.
        channel.CallbackExceptionAsync += (_, e) =>
        {
            Log.Write(connection.ClientProvidedName ?? "rabbit", $"исключение в обработчике: {e.Exception.Message}");
            return Task.CompletedTask;
        };

        return channel;
    }

    // Объявления идемпотентны, поэтому каждый режим вызывает это при старте.
    public static async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(MailExchange, ExchangeType.Direct, durable: true);
        await channel.ExchangeDeclareAsync(MailDeadLetterExchange, ExchangeType.Fanout, durable: true);

        // delivery-limit считает reject с requeue и падения потребителя, nack — нет (RabbitMQ 4.3+).
        await channel.QueueDeclareAsync(MailQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-delivery-limit"] = MailDeliveryLimit,
                ["x-dead-letter-exchange"] = MailDeadLetterExchange,
                // Письмо остаётся в q.mail, пока DLQ не подтвердит приём; по умолчанию (at-most-once) могло бы пропасть.
                // Работает только вместе с x-overflow = reject-publish.
                ["x-dead-letter-strategy"] = "at-least-once",
                ["x-overflow"] = "reject-publish",
            });
        await channel.QueueBindAsync(MailQueue, MailExchange, MailRoutingKey);

        // Quorum-очередь хранит на диске и непостоянные сообщения (например, из UI): ждущее разбора не пропадёт.
        await channel.QueueDeclareAsync(MailDeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        await channel.QueueBindAsync(MailDeadLetterQueue, MailDeadLetterExchange, routingKey: "");

        // Fanout: каждая привязанная очередь получает копию. Ненужные типы событий WebhookSender пропускает сам.
        await channel.ExchangeDeclareAsync(EventsExchange, ExchangeType.Fanout, durable: true);

        // Негодные события и исчерпавшие delivery-limit (по умолчанию 20) уходят в q.webhook.failed
        // через default exchange, где routing key = имя очереди.
        await channel.QueueDeclareAsync(WebhookQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = WebhookFailedQueue,
                ["x-dead-letter-strategy"] = "at-least-once",
                ["x-overflow"] = "reject-publish",
            });
        await channel.QueueBindAsync(WebhookQueue, EventsExchange, routingKey: "");

        // DLX нет: Audit ничего не отклоняет.
        await channel.QueueDeclareAsync(AuditQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        await channel.QueueBindAsync(AuditQueue, EventsExchange, routingKey: "");

        // "Зал ожидания" без потребителей: по истечении TTL брокер возвращает сообщение в q.webhook.
        // Classic-очередь перекладывает мёртвые сообщения только в режиме at-most-once.
        await channel.QueueDeclareAsync(WebhookRetryQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-message-ttl"] = (int)WebhookRetryDelay.TotalMilliseconds,
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = WebhookQueue,
            });

        // Parking lot: вебхуки, которые так и не ушли, и негодные события. Разбирает человек.
        await channel.QueueDeclareAsync(WebhookFailedQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
    }

    public static BasicProperties GetProps(Guid messageId, string? type = null) => new()
    {
        Persistent = true,
        MessageId = messageId.ToString(),
        ContentType = "application/json",
        Type = type,
    };

    // Число в заголовке может прийти как int или long.
    public static int GetIntHeader(IReadOnlyBasicProperties props, string name, int defaultValue = 0) =>
        props.Headers?.TryGetValue(name, out var value) == true
            ? value switch { int i => i, long l => (int)l, _ => defaultValue }
            : defaultValue;

    // Если канал уже закрыт, ответ бросит исключение: его залогирует CallbackExceptionAsync,
    // а брокер сам вернёт неподтверждённое сообщение в очередь.
    public static async Task RespondAsync(IChannel channel, ulong deliveryTag, OutcomeStatus outcome)
    {
        switch (outcome)
        {
            case OutcomeStatus.Ack: await channel.BasicAckAsync(deliveryTag, multiple: false); break;
            case OutcomeStatus.NackRequeue: await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: true); break;
            case OutcomeStatus.RejectRequeue: await channel.BasicRejectAsync(deliveryTag, requeue: true); break;
            case OutcomeStatus.RejectDead: await channel.BasicRejectAsync(deliveryTag, requeue: false); break;
        }
    }
}
