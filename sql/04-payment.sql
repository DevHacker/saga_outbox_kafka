\connect payment_db

CREATE TABLE payments (
    order_id   uuid PRIMARY KEY,
    amount     numeric(12, 2) NOT NULL,
    status     text           NOT NULL,
    created_at timestamptz    NOT NULL
);

CREATE TABLE outbox_event (
    id           uuid PRIMARY KEY,
    aggregate_id text        NOT NULL,
    topic        text        NOT NULL,
    type         text        NOT NULL,
    payload      text        NOT NULL,
    created_at   timestamptz NOT NULL
);

CREATE TABLE processed_event (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL
);
