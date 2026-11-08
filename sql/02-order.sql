\connect order_db

CREATE TABLE orders (
    id         uuid PRIMARY KEY,
    item       text           NOT NULL,
    quantity   int            NOT NULL,
    amount     numeric(12, 2) NOT NULL,
    status     text           NOT NULL,
    created_at timestamptz    NOT NULL
);

-- Colonnes lues par l'EventRouter de Debezium :
--   id           -> header Kafka "id"        (idempotence côté consommateur)
--   aggregate_id -> clé du message Kafka     (même commande = même partition = ordre garanti)
--   topic        -> topic de destination
--   type         -> header Kafka "eventType"
--   payload      -> valeur du message Kafka  (JSON)
CREATE TABLE outbox_event (
    id           uuid PRIMARY KEY,
    aggregate_id text        NOT NULL,
    topic        text        NOT NULL,
    type         text        NOT NULL,
    payload      text        NOT NULL,
    created_at   timestamptz NOT NULL
);

-- Inbox : événements déjà traités par CE service
CREATE TABLE processed_event (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL
);
