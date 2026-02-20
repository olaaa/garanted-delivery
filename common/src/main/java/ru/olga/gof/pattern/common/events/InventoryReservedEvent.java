package ru.olga.gof.pattern.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Событие успешного резервирования товара на складе.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservedEvent {

    private String orderId;
    private String sagaId;
    private String productCode;
    private Integer quantity;
    private String reservationId;
    private String idempotencyKey;
    private Instant timestamp;
}
