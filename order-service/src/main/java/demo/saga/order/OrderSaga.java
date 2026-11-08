package demo.saga.order;

import demo.saga.common.events.Event;
import demo.saga.common.Inbox;
import demo.saga.common.KafkaEvents;
import demo.saga.common.Messages.*;
import demo.saga.common.Outbox;
import demo.saga.common.Topics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;


/**
 * ORCHESTRATEUR : écoute les réponses d'Inventory et de Payment, fait avancer la machine à états
 * de la commande, et émet la commande suivante (ou la compensation) via son outbox.
 *
 *   PENDING --InventoryReserved--> INVENTORY_RESERVED --PaymentCompleted--> COMPLETED
 *   PENDING --InventoryRejected--> REJECTED
 *   INVENTORY_RESERVED --PaymentFailed--> COMPENSATING --InventoryReleased--> CANCELLED
 */
@Service
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);
    private static final String CONSUMER = "order-saga";

    private final KafkaEvents kafka;
    private final Inbox inbox;
    private final OrderService orderService;
    private final R2dbcEntityTemplate template;
    private final Outbox outbox;
    private Disposable subscription;

    public OrderSaga(KafkaEvents kafka, Inbox inbox, OrderService orderService,
                     R2dbcEntityTemplate template, Outbox outbox) {
        this.kafka = kafka;
        this.inbox = inbox;
        this.orderService = orderService;
        this.template = template;
        this.outbox = outbox;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        subscription = kafka.listen(CONSUMER, List.of(Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS), this::handle);
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    private Mono<Void> handle(Event event) {
        return inbox.insertProcessedEvent(event.id(), Mono.defer(() -> dispatch(event)));
    }

    /** Le header eventType (= colonne `type` de l'outbox) dit quel record JSON désérialiser. */
    private Mono<Void> dispatch(Event event) {
        return switch (event.type()) {
            case "InventoryReservedEvent" -> onInventoryReserved(event.read(InventoryReservedEvent.class));
            case "InventoryRejectedEvent" -> transition(event.read(InventoryRejectedEvent.class).orderId(), Order.Status.REJECTED);
            case "PaymentCompletedEvent"  -> transition(event.read(PaymentCompletedEvent.class).orderId(), Order.Status.COMPLETED);
            case "PaymentFailedEvent"     -> onPaymentFailed(event.read(PaymentFailedEvent.class));
            case "InventoryReleasedEvent" -> transition(event.read(InventoryReleasedEvent.class).orderId(), Order.Status.CANCELLED);
            default -> Mono.fromRunnable(() -> log.warn("type d'événement inconnu : {}", event.type()));
        };
    }

    private Mono<Void> onInventoryReserved(InventoryReservedEvent event) {
        return updateOrder(event.orderId(), Order.Status.INVENTORY_RESERVED)
                .flatMap(o -> outbox.publish(o.id(), Topics.PAYMENT_COMMANDS,
                        new ProcessPaymentCommand(o.id(), o.amount())));
    }

    private Mono<Void> onPaymentFailed(PaymentFailedEvent event) {
        return updateOrder(event.orderId(), Order.Status.COMPENSATING)
                .flatMap(o -> outbox.publish(o.id(), Topics.INVENTORY_COMMANDS,
                        new ReleaseInventoryCommand(o.id())));
    }

    private Mono<Void> transition(UUID orderId, Order.Status status) {
        return updateOrder(orderId, status).then();
    }

    private Mono<Order> updateOrder(UUID orderId, Order.Status status) {
        return orderService.findOrderById(orderId)
                .flatMap(foundOrder -> template.update(foundOrder.withStatus(status)))
                .doOnNext(foundOrder -> log.info("commande {} -> {}", foundOrder.id(), foundOrder.status()));
    }
}
