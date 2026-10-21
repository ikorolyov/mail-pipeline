-- Inbox: какие сообщения какой потребитель уже обработал. Пишется в одной транзакции с результатом обработки.
CREATE TABLE inbox (
    consumer   text NOT NULL,
    message_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Outbox: события, записанные вместе с бизнес-данными и ещё не опубликованные.
-- message_id — id события, Relay публикует его как MessageId.
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    message_id uuid NOT NULL,
    type       text NOT NULL,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

-- Relay ищет только неотправленные строки.
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;
