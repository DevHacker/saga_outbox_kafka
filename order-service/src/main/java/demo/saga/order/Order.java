package demo.saga.order;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Table("orders")
public record Order(
        @Id UUID id,
        String item,
        int quantity,
        BigDecimal amount,
        Status status,
        Instant createdAt) {

    /** États de la saga (le record est immuable : on crée une copie à chaque transition). */
    public enum Status { PENDING, INVENTORY_RESERVED, READY_FOR_DELIVERY, COMPLETED, REJECTED, COMPENSATING, CANCELLED }

    public Order withStatus(Status newStatus) {
        return new Order(id, item, quantity, amount, newStatus, createdAt);
    }
}
