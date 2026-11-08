package demo.saga.common;

import demo.saga.common.events.OutboxEvent;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Côté ÉCRITURE : le service n'appelle JAMAIS Kafka. Il insère une ligne dans SA table `outbox_event`,
 * dans la même transaction que son changement d'état. Son connecteur Debezium fait le reste.
 */
@Component
public class Outbox {

    private final R2dbcEntityTemplate template;

    public Outbox(R2dbcEntityTemplate template) {
        this.template = template;
    }

    public Mono<Void> publish(UUID aggregateId, String topic, Object message) {
        return Mono.defer(() -> template.insert(new OutboxEvent(
                        UUID.randomUUID(),
                        aggregateId.toString(),
                        topic,
                        message.getClass().getSimpleName(),
                        JsonCodec.write(message),
                        Instant.now()))
                .then());
    }
}
