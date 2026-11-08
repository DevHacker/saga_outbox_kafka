package demo.saga.inventory;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

@Table("reservations")
public record Reservation(@Id UUID orderId, String item, int quantity) {
}
