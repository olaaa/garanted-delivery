package ru.olga.gof.pattern.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Событие создания заказа.
 * Публикуется Order Service после создания заказа.
 *
 * ВАЖНО ДЛЯ ГАРАНТИРОВАННОЙ ДОСТАВКИ:
 * - Содержит idempotencyKey для дедупликации (exactly-once semantics)
 * - Timestamp для ordering и отладки
 * - Все необходимые данные для обработки без дополнительных запросов
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    /**
     * Уникальный идентификатор заказа
     */
    private String orderId;

    /**
     * Идентификатор клиента
     */
    private String customerId;

    /**
     * Код товара для резервирования
     */
    private String productCode;

    /**
     * Количество единиц товара
     */
    private Integer quantity;

    /**
     * Общая сумма заказа
     */
    private BigDecimal totalAmount;

    /**
     * Идентификатор для дедупликации (exactly-once).
     * Обычно = orderId, но может быть составным ключом.
     */
    private String idempotencyKey;

    /**
     * Временная метка создания события
     */
    private Instant timestamp;

    /**
     * ID саги (для связи всех событий в рамках одной транзакции)
     */
    private String sagaId;
}
