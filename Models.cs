using System.Text.Json;
using System.Text.Json.Serialization;

namespace MailPipeline;

public enum MailKind { Reply, Bounce, Noise, Broken }

// "Скачанное письмо" от загрузчика почты. MessageId — id письма и сообщения в RabbitMQ, по нему Processor отсекает дубли.
public record MailDownloaded(Guid MessageId, int AccountId, MailKind Kind, string From, string Subject);

// EventId — id события (MessageId при публикации), MailId — исходное письмо.
// Id разные, иначе два события об одном письме выглядели бы для inbox как дубли.
public record ReplyReceived(Guid EventId, Guid MailId, int AccountId, string From);

public record OutboxModel(long Id, Guid MessageId, string Type, string Payload);

public static class Json
{
    private static readonly JsonSerializerOptions Options = new()
    {
        Converters = { new JsonStringEnumConverter(allowIntegerValues: false) },
        // Строгий разбор: без обязательного поля или с null вместо строки — JsonException,
        // а не молчаливые Guid.Empty, 0 и null.
        RespectRequiredConstructorParameters = true,
        RespectNullableAnnotations = true,
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static byte[] ToBytes<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static T FromBytes<T>(ReadOnlyMemory<byte> body) where T : class =>
        JsonSerializer.Deserialize<T>(body.Span, Options) ?? throw new JsonException("тело сообщения — null");
}

// Формат строки: время [компонент] id что произошло -> ответ брокеру.
public static class Log
{
    public static string Id(Guid id) => id.ToString()[..8];

    // MessageId из свойств сообщения: при публикации из UI там может быть что угодно или ничего.
    public static string Id(string? id) => string.IsNullOrEmpty(id) ? "(нет id)" : id[..Math.Min(8, id.Length)];

    public static void Write(string component, string text) =>
        Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {"[" + component + "]",-11} {text}");
}
