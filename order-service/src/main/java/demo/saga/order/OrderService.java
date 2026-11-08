package demo.saga.order;
import demo.saga.common.Messages.ReserveInventoryCommand;
import demo.saga.common.Outbox;
import demo.saga.common.Topics;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

@Service
public class OrderService {


    public record CreateOrder(String item, int quantity, BigDecimal amount) {}

    private final R2dbcEntityTemplate r2dbcEntityTemplate;
    private final Outbox outbox;
    private final TransactionalOperator tx;

    public OrderService(R2dbcEntityTemplate r2dbcEntityTemplate, Outbox outbox, TransactionalOperator tx) {
        this.r2dbcEntityTemplate = r2dbcEntityTemplate;
        this.outbox = outbox;
        this.tx = tx;
    }

    /** Étape 0 de la saga : commande PENDING + commande ReserveInventory dans l'outbox, UNE seule transaction. */
    public Mono<Order> create(CreateOrder cmd) {
        var order = new Order(UUID.randomUUID(), cmd.item(), cmd.quantity(), cmd.amount(),
                Order.Status.PENDING, Instant.now());

        Mono<Order> work = r2dbcEntityTemplate.insert(order)   // insert() force un INSERT (save() ferait un UPDATE : id déjà renseigné)
                .delayUntil(o -> outbox.publish(o.id(), Topics.INVENTORY_COMMANDS,
                        new ReserveInventoryCommand(o.id(), o.item(), o.quantity())));
        return tx.transactional(work);
    }

    public Mono<Order> findOrderById(UUID id) {
        return r2dbcEntityTemplate.selectOne(query(where("id").is(id)), Order.class);
    }
}
