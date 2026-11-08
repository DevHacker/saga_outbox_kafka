package demo.saga.inventory;

import demo.saga.common.events.Event;
import demo.saga.common.Inbox;
import demo.saga.common.KafkaEvents;
import demo.saga.common.Messages.*;
import demo.saga.common.Outbox;
import demo.saga.common.Topics;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

/** Consomme inventory.commands, répond dans inventory.events (via son propre outbox). */
@Service
public class InventoryService {

    private static final String CONSUMER_GROUP_ID = "inventory-service";

    private final KafkaEvents kafka;
    private final Inbox inbox;
    private final Outbox outbox;
    private final R2dbcEntityTemplate template;
    private final DatabaseClient db;
    private Disposable subscription;
    private final TransactionalOperator transactionalOperator;

    public InventoryService(KafkaEvents kafka,
                            Inbox inbox,
                            Outbox outbox,
                            R2dbcEntityTemplate template,
                            DatabaseClient db,
                            TransactionalOperator transactionalOperator) {
        this.kafka = kafka;
        this.inbox = inbox;
        this.outbox = outbox;
        this.template = template;
        this.db = db;
        this.transactionalOperator = transactionalOperator;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        subscription = kafka.listen(CONSUMER_GROUP_ID, List.of(Topics.INVENTORY_COMMANDS), this::handle);
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    private Mono<Void> handle(Event event) {
        return inbox.insertProcessedEvent(event.id(), Mono.defer(() -> dispatch(event)));
    }

    private Mono<Void> dispatch(Event event) {
        return switch (event.type()) {
            case "ReserveInventoryCommand" -> reserve(event.read(ReserveInventoryCommand.class));
            case "ReleaseInventoryCommand" -> release(event.read(ReleaseInventoryCommand.class));
            default -> Mono.empty();
        };
    }

    private Mono<Void> reserve(ReserveInventoryCommand cmd) {
        // Décrément atomique et conditionnel : pas de lecture-puis-écriture, donc pas de course entre commandes.
        return transactionalOperator.transactional(
                db.sql("UPDATE stock SET available = available - :qty WHERE item = :item AND available >= :qty")
                .bind("qty", cmd.quantity())
                .bind("item", cmd.item())
                .fetch()
                .rowsUpdated()
                .flatMap(updated -> updated == 1
                        ? template.insert(new Reservation(cmd.orderId(), cmd.item(), cmd.quantity()))
                                .then(publishToOutBox(cmd.orderId(), new InventoryReservedEvent(cmd.orderId())))
                        : publishToOutBox(cmd.orderId(), new InventoryRejectedEvent(cmd.orderId(), "stock insuffisant"))));
    }

    /** Compensation : rend le stock, supprime la réservation. Idempotent si la réservation n'existe plus. */
    private Mono<Void> release(ReleaseInventoryCommand cmd) {
        return transactionalOperator.transactional(
                template.selectOne(query(where("orderId").is(cmd.orderId())), Reservation.class)
                .flatMap(r -> db.sql("UPDATE stock SET available = available + :qty WHERE item = :item")
                        .bind("qty", r.quantity())
                        .bind("item", r.item())
                        .fetch()
                        .rowsUpdated()
                        .then(template.delete(r)))
                .then(publishToOutBox(cmd.orderId(), new InventoryReleasedEvent(cmd.orderId()))));
    }

    private Mono<Void> publishToOutBox(UUID orderId, Object message) {
        return outbox.publish(orderId, Topics.INVENTORY_EVENTS, message);
    }
}
