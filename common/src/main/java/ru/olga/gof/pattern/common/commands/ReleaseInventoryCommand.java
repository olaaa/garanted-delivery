package ru.olga.gof.pattern.common.commands;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * COMPENSATING COMMAND: освобождение резервирования товара.
 * Отправляется оркестратором при откате саги.
 *
 * Компенсирует: ReserveInventoryCommand → InventoryReservedEvent
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReleaseInventoryCommand {

    private String orderId;
    private String sagaId;
    private String reservationId;
    private String productCode;
    private Integer quantity;
    private String reason;
    private String idempotencyKey;
    private Instant timestamp;
}
