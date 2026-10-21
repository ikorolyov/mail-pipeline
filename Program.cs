using MailPipeline;

Console.OutputEncoding = System.Text.Encoding.UTF8;

switch (args.FirstOrDefault())
{
    case "generator": await Generator.RunAsync(args[1..]); break;
    case "processor": await Processor.RunAsync(); break;
    case "relay": await Relay.RunAsync(args[1..]); break;
    case "webhook": await WebhookSender.RunAsync(); break;
    case "audit": await Audit.RunAsync(); break;
    default:
        Console.WriteLine("Использование: dotnet run -- <режим>");
        Console.WriteLine("  generator [N] [--dup]  опубликовать N писем (по умолчанию 20); --dup — каждое 5-е дважды");
        Console.WriteLine("  processor              обрабатывать q.mail, события писать в outbox");
        Console.WriteLine("  relay [--slow]         публиковать outbox в x.events; --slow — пачки по 5, пауза 1 с");
        Console.WriteLine("  webhook                отправлять вебхуки с повторами");
        Console.WriteLine("  audit                  печатать все события");
        return 1;
}

return 0;
