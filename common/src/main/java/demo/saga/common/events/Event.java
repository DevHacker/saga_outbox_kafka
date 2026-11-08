package demo.saga.common.events;

import demo.saga.common.JsonCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Ce que le consommateur voit d'un message produit par Debezium (après l'EventRouter) :
 *   clé          = colonne aggregate_id
 *   header "id"  = colonne id          (UUID de la ligne outbox)
 *   header "eventType" = colonne type
 *   valeur       = colonne payload     (JSON)
 */
public record Event(UUID id, String topic, String type, String key, String payload) {

    public static Event from(ConsumerRecord<String, String> record) {
        return new Event(
                UUID.fromString(headerReader(record, "id")),
                record.topic(),
                headerReader(record, "eventType"),
                record.key(),
                record.value());
    }

    public <T> T read(Class<T> messageType) {
        return JsonCodec.read(payload, messageType);
    }

    private static String headerReader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalStateException("header Kafka manquant : " + name);
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
