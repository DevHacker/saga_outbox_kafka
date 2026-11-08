package demo.saga.common;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Contrats de la saga. Le nom simple du record devient le `type` de l'outbox,
 * donc le header Kafka `eventType` que le consommateur lit pour choisir quoi désérialiser.
 */
public final class Messages {

    // Commandes envoyées par l'orchestrateur (Order)
    public record ReserveInventoryCommand(UUID orderId, String item, int quantity) {}
    public record ReleaseInventoryCommand(UUID orderId) {}            // compensation
    public record ProcessPaymentCommand(UUID orderId, BigDecimal amount) {}

    // Réponses d'Inventory
    public record InventoryReservedEvent(UUID orderId) {}
    public record InventoryRejectedEvent(UUID orderId, String reason) {}
    public record InventoryReleasedEvent(UUID orderId) {}

    // Réponses de Payment
    public record PaymentCompletedEvent(UUID orderId) {}
    public record PaymentFailedEvent(UUID orderId, String reason) {}

    private Messages() {
    }
}
