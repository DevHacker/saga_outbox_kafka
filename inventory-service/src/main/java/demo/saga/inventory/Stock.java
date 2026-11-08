package demo.saga.inventory;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("stock")
public record Stock(@Id String item, int available) {
}
