package demo.saga.order;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.net.URI;
import java.util.UUID;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.created;
import static org.springframework.web.reactive.function.server.ServerResponse.notFound;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@Configuration
public class OrderRoutes {

    @Bean
    RouterFunction<ServerResponse> orderRouter(OrderService orders) {
        return route()
                .POST("/orders", req -> req.bodyToMono(OrderService.CreateOrder.class)
                        .flatMap(orders::create)
                        .flatMap(o -> created(URI.create("/orders/" + o.id())).bodyValue(o)))
                .GET("/orders/{id}", req -> orders.findOrderById(UUID.fromString(req.pathVariable("id")))
                        .flatMap(o -> ok().bodyValue(o))
                        .switchIfEmpty(notFound().build()))
                .build();
    }
}
