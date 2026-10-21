using RabbitMQ.Client;

namespace MailPipeline;

// Имитирует загрузчик почты: публикует "скачанные письма" в x.mail и завершается.
public static class Generator
{
    private const string ClientName = "generator";

    public static async Task RunAsync(string[] args)
    {
        var count = args.Length > 0 && int.TryParse(args[0], out var n) ? n : 20;
        var withDuplicates = args.Contains("--dup");

        await using var connection = await Rabbit.ConnectAsync(ClientName);
        await using var channel = await Rabbit.CreateChannelAsync(connection, confirms: true);
        await Rabbit.DeclareTopologyAsync(channel);

        var duplicates = 0;
        for (var i = 1; i <= count; i++)
        {
            var mail = new MailDownloaded(Guid.NewGuid(), Random.Shared.Next(1, 6), PickKind(), $"contact{i}@example.com", $"Re: предложение #{i}");
            var line = $"{Log.Id(mail.MessageId)} {mail.Kind,-6} Account={mail.AccountId} -> {Rabbit.MailExchange}";

            await PublishAsync(channel, mail);
            Log.Write(ClientName, line);

            if (withDuplicates && i % 5 == 0)
            {
                await PublishAsync(channel, mail);
                duplicates++;
                Log.Write(ClientName, $"{line} (дубль)");
            }
        }

        Log.Write(ClientName, $"опубликовано писем: {count}{(duplicates > 0 ? $", дублей: {duplicates}" : "")}, все подтверждены");
    }

    private static async Task PublishAsync(IChannel channel, MailDownloaded mail)
    {
        // mandatory: если письмо некуда маршрутизировать, брокер вернёт его, и вызов бросит исключение.
        await channel.BasicPublishAsync(Rabbit.MailExchange, Rabbit.MailRoutingKey, mandatory: true,
            basicProperties: Rabbit.GetProps(mail.MessageId), body: Json.ToBytes(mail));
    }

    // 70% ответов, 15% баунсов, 10% шума, 5% "битых" писем, на которых обработчик падает при каждой попытке.
    private static MailKind PickKind() => Random.Shared.Next(100) switch
    {
        < 70 => MailKind.Reply,
        < 85 => MailKind.Bounce,
        < 95 => MailKind.Noise,
        _ => MailKind.Broken,
    };
}
