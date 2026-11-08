package demo.saga.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// "demo.saga.common" : composants partagés (Outbox, Inbox, KafkaEvents) de la lib common
@SpringBootApplication(scanBasePackages = {"demo.saga.order", "demo.saga.common"})
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
