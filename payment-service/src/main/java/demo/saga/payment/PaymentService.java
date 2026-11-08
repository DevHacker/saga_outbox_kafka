package demo.saga.payment;

import demo.saga.common.*;
import demo.saga.common.Messages.PaymentCompletedEvent;
import demo.saga.common.Messages.PaymentFailedEvent;
import demo.saga.common.Messages.ProcessPaymentCommand;
import demo.saga.common.events.Event;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Consomme payment.commands, répond dans payment.replies. Paiement simulé : refusé au-dessus de 1000. */
@Service
public class PaymentService {

    private static final String CONSUMER = "payment-service";
    private static final BigDecimal LIMIT = new BigDecimal("1000");

    private final KafkaEvents kafka;
    private final Inbox inbox;
    private final Outbox outbox;
    private final R2dbcEntityTemplate template;
    private Disposable subscription;

    public PaymentService(KafkaEvents kafka, Inbox inbox, Outbox outbox, R2dbcEntityTemplate template) {
        this.kafka = kafka;
        this.inbox = inbox;
        this.outbox = outbox;
        this.template = template;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        subscription = kafka.listen(CONSUMER, List.of(Topics.PAYMENT_COMMANDS), this::handle);
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    private Mono<Void> handle(Event event) {
        return inbox.insertProcessedEvent(event.id(), Mono.defer(() -> dispatch(event)));
    }

    private Mono<Void> dispatch(Event event) {
        return switch (event.type()) {
            case "ProcessPaymentCommand" -> process(event.read(ProcessPaymentCommand.class));
            default -> Mono.empty();
        };
    }

    private Mono<Void> process(ProcessPaymentCommand cmd) {
        boolean accepted = cmd.amount().compareTo(LIMIT) <= 0;
        var payment = new Payment(cmd.orderId(), cmd.amount(),
                accepted ? Payment.Status.COMPLETED : Payment.Status.FAILED, Instant.now());
        var processResult = accepted
                ? new PaymentCompletedEvent(cmd.orderId())
                : new PaymentFailedEvent(cmd.orderId(), "montant supérieur à la limite");

        return template
                .insert(payment)
                .then(outbox.publish(cmd.orderId(), Topics.PAYMENT_EVENTS, processResult));
    }
}
