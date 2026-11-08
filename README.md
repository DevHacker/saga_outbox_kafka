# Saga orchestrator minimale — Kafka + Debezium (outbox CDC)

Java 21 · Spring Boot 4.1 · WebFlux (endpoints fonctionnels) · R2DBC/PostgreSQL · reactor-kafka · Debezium 3.0

Trois services indépendants (une application Spring Boot chacune), plus la lib partagée `common` (code seulement, aucune donnée).
`order-service` est l'orchestrateur.

| Service | Port | Base | Tables | Connecteur Debezium | Slot de réplication |
|---|---|---|---|---|---|
| order-service | 8080 | `order_db` | `orders`, `outbox_event`, `processed_event` | `order-outbox-connector` | `order_outbox_slot` |
| inventory-service | 8081 | `inventory_db` | `stock`, `reservations`, `outbox_event`, `processed_event` | `inventory-outbox-connector` | `inventory_outbox_slot` |
| payment-service | 8082 | `payment_db` | `payments`, `outbox_event`, `processed_event` | `payment-outbox-connector` | `payment_outbox_slot` |

Les 3 bases tournent sur la même instance Postgres pour la démo ; en vrai, trois instances.

## Lancer

```bash
docker compose up -d            # postgres (3 bases créées par sql/0*.sql) + kafka + kafka connect

# un connecteur par service (attendre ~20 s que Connect démarre)
for s in order inventory payment; do
  curl -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @connector/$s-connector.json
done
curl localhost:8083/connectors?expand=status             # 3 connecteurs RUNNING attendus

mvn -DskipTests package
java -jar order-service/target/order-service-0.0.1-SNAPSHOT.jar &
java -jar inventory-service/target/inventory-service-0.0.1-SNAPSHOT.jar &
java -jar payment-service/target/payment-service-0.0.1-SNAPSHOT.jar &
```

## Scénarios

```bash
# 1. nominal : COMPLETED
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"item":"laptop","quantity":1,"amount":500}'

# 2. paiement refusé (> 1000) : compensation, état final CANCELLED, stock rendu
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"item":"laptop","quantity":1,"amount":5000}'

# 3. stock insuffisant : REJECTED
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"item":"phone","quantity":50,"amount":100}'

curl localhost:8080/orders/<id>        # état de la saga
curl localhost:8081/inventory/laptop   # stock
curl localhost:8082/payments/<id>      # paiement
```

## Le chemin d'un événement (ce qu'il faut regarder)

```
order-service : OrderService.create()          une transaction SQL sur order_db :
  INSERT orders (PENDING)                        - changement d'état
  INSERT outbox_event (ReserveInventory)         - + message à publier
        │
        │  WAL PostgreSQL de order_db (logical decoding, pgoutput, slot order_outbox_slot)
        ▼
Debezium : order-outbox-connector  ── lit l'INSERT dans order_db.outbox_event
        │  event brut (enveloppe Debezium) :
        │    {"before":null,
        │     "after":{"id":"…","aggregate_id":"<orderId>","topic":"inventory.commands",
        │              "type":"ReserveInventory","payload":"{…}","created_at":…},
        │     "source":{"db":"order_db","table":"outbox_event","lsn":…}, "op":"c"}
        │
        │  SMT EventRouter (transforms.outbox.*) transforme en :
        │    topic    = colonne `topic`          -> inventory.commands
        │    clé      = colonne `aggregate_id`   -> <orderId>   (même commande = même partition = ordre)
        │    header id        = colonne `id`
        │    header eventType = colonne `type`   -> ReserveInventory
        │    valeur   = colonne `payload`        -> {"orderId":"…","item":"laptop","quantity":1}
        ▼
Kafka topic inventory.commands
        │
        ▼
inventory-service : KafkaEvents.listen()       flux réactif reactor-kafka, concatMap = un message à la fois
  Event.from(record)                             lit clé + headers id / eventType + valeur
  Inbox.once(event.id(), work)                   transaction sur inventory_db :
                                                   INSERT processed_event ON CONFLICT DO NOTHING
                                                   0 ligne = doublon -> ignoré ; sinon work
  InventoryService.dispatch(event)               switch sur eventType -> désérialise le bon record
     reserve() : UPDATE stock … + INSERT reservations + INSERT outbox_event (InventoryReserved)
  offset acquitté APRÈS le traitement
        │
        │  inventory-outbox-connector lit inventory_db.outbox_event -> topic inventory.replies
        ▼
order-service : OrderSaga  -> avance l'état, INSERT outbox_event (ProcessPayment) -> payment.commands
        │
        └─> payment-service (payment-outbox-connector) -> payment.replies -> OrderSaga -> COMPLETED / compensation
```

Voir les messages bruts dans Kafka (headers inclus) :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic inventory.commands --from-beginning \
  --property print.key=true --property print.headers=true
```

Voir l'outbox d'un service : `docker compose exec postgres psql -U postgres order_db -c "select * from outbox_event"`.

## Points à retenir

- Isolation : un service n'a accès qu'à sa base. Il ne voit les autres que par Kafka.
- Un connecteur par base : `database.dbname`, `topic.prefix`, `slot.name` et `publication.name` doivent être uniques par connecteur (un slot par base, noms uniques dans le cluster).
- Le service n'appelle jamais Kafka : il écrit en base. Atomicité état + message garantie par la transaction locale.
- Livraison at-least-once : d'où `processed_event`, indexée par l'`id` de la ligne `outbox_event` (header `id`).
- Saga : `Order.Status` est la machine à états. Échec paiement -> `COMPENSATING` -> commande `ReleaseInventory` -> `CANCELLED`.
- `common` est une lib de code (Outbox, Inbox, KafkaEvents, contrats de messages). Chaque service l'active avec `scanBasePackages`.
- Records : `template.insert(record)` force un INSERT (`save()` ferait un UPDATE car l'id est déjà renseigné) ; les transitions créent une copie (`withStatus`) puis `template.update`.
- `kafka.version` épinglé à 3.9.1 dans le pom parent : reactor-kafka 1.3.x n'est pas compilé contre kafka-clients 4.x (version gérée par Boot 4.1).
- Volontairement absent : Flyway, purge de l'outbox, DLQ, tests, sécurité, garde-fous sur les transitions d'état.
