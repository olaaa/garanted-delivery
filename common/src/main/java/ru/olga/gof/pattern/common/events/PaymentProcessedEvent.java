package ru.olga.gof.pattern.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Событие успешной обработки платежа.
 * Публикуется Payment Service после успешного списания средств.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentProcessedEvent {

    private String orderId;
    private String sagaId;
    private String paymentId;
    private BigDecimal amount;
    private String customerId;
    private String idempotencyKey;
    private Instant timestamp;

    /**
     * Данные для возможной компенсации (refund)
     */
    private String paymentMethod;
    private String transactionId;
}
