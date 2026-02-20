package ru.olga.gof.pattern.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Событие ошибки обработки платежа.
 * Триггер для начала компенсации (rollback) саги.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentFailedEvent {

    private String orderId;
    private String sagaId;
    private String customerId;
    private String reason;
    private String idempotencyKey;
    private Instant timestamp;
}
