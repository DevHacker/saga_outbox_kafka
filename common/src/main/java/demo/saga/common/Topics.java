package demo.saga.common;

/** Topics Kafka. Ils sont écrits dans la colonne `topic` de l'outbox : Debezium route le message vers ce topic. */
public final class Topics {
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String PAYMENT_EVENTS = "payment.events";

    private Topics() {
    }
}
