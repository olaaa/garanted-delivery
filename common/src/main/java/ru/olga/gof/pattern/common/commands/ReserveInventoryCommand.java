package ru.olga.gof.pattern.common.commands;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Команда для резервирования товара на складе.
 * Отправляется Saga Orchestrator → Inventory Service.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReserveInventoryCommand {

    private String orderId;
    private String sagaId;
    private String productCode;
    private Integer quantity;
    private String idempotencyKey;
    private Instant timestamp;
}
