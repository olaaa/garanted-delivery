package ru.olga.gof.pattern.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {

    private String customerId;
    private String productCode;
    private Integer quantity;
    private BigDecimal totalAmount;
}
