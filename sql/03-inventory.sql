\connect inventory_db

CREATE TABLE stock (
    item      text PRIMARY KEY,
    available int NOT NULL
);

CREATE TABLE reservations (
    order_id uuid PRIMARY KEY,
    item     text NOT NULL,
    quantity int  NOT NULL
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

INSERT INTO stock (item, available) VALUES ('laptop', 10), ('phone', 2);
