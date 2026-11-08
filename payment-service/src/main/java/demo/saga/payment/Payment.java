package demo.saga.payment;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Table("payments")
public record Payment(@Id UUID orderId, BigDecimal amount, Status status, Instant createdAt) {

    public enum Status { COMPLETED, FAILED }
}
