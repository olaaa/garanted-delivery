package ru.olga.gof.pattern.common.model;

/**
 * Статусы выполнения саги
 */
public enum SagaStatus {
    /**
     * Сага создана, ожидает начала выполнения
     */
    STARTED,

    /**
     * Обрабатывается платеж
     */
    PAYMENT_PROCESSING,

    /**
     * Платеж завершен успешно
     */
    PAYMENT_COMPLETED,

    /**
     * Резервируется товар на складе
     */
    INVENTORY_RESERVING,

    /**
     * Сага успешно завершена (все шаги выполнены)
     */
    COMPLETED,

    /**
     * Сага провалилась, началась компенсация
     */
    COMPENSATING,

    /**
     * Компенсация завершена, сага откачена
     */
    COMPENSATED,

    /**
     * Ошибка в процессе компенсации (требуется ручное вмешательство)
     */
    COMPENSATION_FAILED
}
