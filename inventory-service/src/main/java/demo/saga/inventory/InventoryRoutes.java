package demo.saga.inventory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.notFound;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@Configuration
public class InventoryRoutes {

    @Bean
    RouterFunction<ServerResponse> inventoryRouter(R2dbcEntityTemplate template) {
        return route()
                .GET("/inventory/{item}", req -> template
                        .selectOne(query(where("item").is(req.pathVariable("item"))), Stock.class)
                        .flatMap(s -> ok().bodyValue(s))
                        .switchIfEmpty(notFound().build()))
                .build();
    }
}
