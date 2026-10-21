using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MailPipeline;

// Второй подписчик x.events: показывает, что одно событие независимо получают несколько потребителей.
public static class Audit
{
    private const string ConsumerName = "audit";

    public static async Task RunAsync()
    {
        await using var connection = await Rabbit.ConnectAsync(ConsumerName);
        await using var channel = await Rabbit.CreateChannelAsync(connection);
        await Rabbit.DeclareTopologyAsync(channel);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            // Тело не разбираем: упасть нечему, поэтому ответ всегда ack.
            Log.Write(ConsumerName, $"{Log.Id(ea.BasicProperties.MessageId)} {ea.BasicProperties.Type} {Encoding.UTF8.GetString(ea.Body.Span)}");
            await Rabbit.RespondAsync(channel, ea.DeliveryTag, OutcomeStatus.Ack);
        };

        await channel.BasicConsumeAsync(Rabbit.AuditQueue, autoAck: false, consumer);

        Log.Write(ConsumerName, $"слушаю {Rabbit.AuditQueue} (Ctrl+C — выход)");
        await Task.Delay(Timeout.Infinite);
    }
}
