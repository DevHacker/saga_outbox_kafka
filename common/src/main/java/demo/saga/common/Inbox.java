package demo.saga.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Consommateur idempotent. Kafka + Debezium = at-least-once : un événement peut arriver 2 fois.
 * L'id de l'événement est inscrit dans la table `processed_event` DU SERVICE, dans la transaction du traitement :
 * si le traitement échoue, l'inscription est annulée et la relivraison retraitera l'événement.
 */
@Component
public class Inbox {

    private static final Logger log = LoggerFactory.getLogger(Inbox.class);

    private final DatabaseClient db;
    private final TransactionalOperator tx;

    public Inbox(DatabaseClient db, TransactionalOperator tx) {
        this.db = db;
        this.tx = tx;
    }

    public Mono<Void> insertProcessedEvent(UUID eventId, Mono<Void> work) {
        Mono<Void> inTransaction = db
                .sql("""
                        INSERT INTO processed_event (event_id, processed_at) 
                        VALUES (:id, now()) 
                        ON CONFLICT DO NOTHING
                 """)
                .bind("id", eventId)
                .fetch()
                .rowsUpdated()
                .flatMap(inserted -> inserted == 0
                        ? Mono.<Void>fromRunnable(() -> log.info("Event with id {} has already been processed", eventId))
                        : work);
        return tx.transactional(inTransaction);
    }
}
