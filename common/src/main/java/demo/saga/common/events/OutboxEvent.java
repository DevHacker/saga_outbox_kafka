package demo.saga.common.events;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Une ligne de la table `outbox_event`. Chaque service a sa propre base, donc sa propre table
 * du même nom et de même structure : le code est partagé, les données ne le sont pas.
 */
@Table("outbox_event")
public record OutboxEvent(
        @Id UUID id,
        String aggregateId,   // colonne aggregate_id -> clé Kafka
        String topic,         // colonne topic        -> topic de destination
        String type,          // colonne type         -> header eventType
        String payload,       // colonne payload      -> valeur Kafka (JSON)
        Instant createdAt) {
}
