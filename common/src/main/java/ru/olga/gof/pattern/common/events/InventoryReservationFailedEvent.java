package ru.olga.gof.pattern.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Событие ошибки резервирования товара (недостаточно на складе).
 * Триггер для начала компенсации.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservationFailedEvent {

    private String orderId;
    private String sagaId;
    private String productCode;
    private Integer requestedQuantity;
    private Integer availableQuantity;
    private String reason;
    private String idempotencyKey;
    private Instant timestamp;
}
