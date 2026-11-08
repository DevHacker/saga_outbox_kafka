-- Une base par service : aucune table partagée, chaque service a son outbox_event et son processed_event.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
