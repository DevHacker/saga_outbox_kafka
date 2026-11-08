package demo.saga.payment;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.util.UUID;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.notFound;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@Configuration
public class PaymentRoutes {

    @Bean
    RouterFunction<ServerResponse> paymentRouter(R2dbcEntityTemplate template) {
        return route()
                .GET("/payments/{orderId}", req -> template
                        .selectOne(query(where("orderId").is(UUID.fromString(req.pathVariable("orderId")))),
                                Payment.class)
                        .flatMap(p -> ok().bodyValue(p))
                        .switchIfEmpty(notFound().build()))
                .build();
    }
}
