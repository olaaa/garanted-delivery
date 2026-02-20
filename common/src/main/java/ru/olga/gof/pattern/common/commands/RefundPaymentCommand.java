package ru.olga.gof.pattern.common.commands;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * COMPENSATING COMMAND: возврат платежа.
 * Отправляется оркестратором при откате саги.
 *
 * Компенсирует: ProcessPaymentCommand → PaymentProcessedEvent
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RefundPaymentCommand {

    private String orderId;
    private String sagaId;
    private String paymentId;
    private String transactionId;
    private BigDecimal amount;
    private String reason;
    private String idempotencyKey;
    private Instant timestamp;
}
