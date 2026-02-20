package ru.olga.gof.pattern.common.commands;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Команда для обработки платежа.
 * Отправляется Saga Orchestrator → Payment Service.
 *
 * ПАТТЕРН ORCHESTRATION:
 * Оркестратор явно отправляет команды участникам саги.
 * Каждая команда содержит sagaId для корреляции.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessPaymentCommand {

    private String orderId;
    private String sagaId;
    private String customerId;
    private BigDecimal amount;
    private String idempotencyKey;
    private Instant timestamp;
}
