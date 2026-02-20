package ru.olga.gof.pattern.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Модель состояния саги (Event Sourcing элемент).
 * Хранится в оркестраторе для отслеживания прогресса и организации компенсаций.
 *
 * ПОЧЕМУ ВАЖНО:
 * - Позволяет восстановить состояние после сбоя оркестратора
 * - Знаем, какие шаги выполнены → какие нужно откатить
 * - Идемпотентность: можем повторно обработать событие
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaState {

    private String sagaId;
    private String orderId;
    private SagaStatus status;

    /**
     * Шаги саги, которые были успешно выполнены.
     * Используются для определения последовательности компенсаций.
     */
    @Builder.Default
    private List<String> completedSteps = new ArrayList<>();

    /**
     * История событий (event sourcing).
     * Можно использовать для восстановления состояния.
     */
    @Builder.Default
    private List<String> eventLog = new ArrayList<>();

    private Instant createdAt;
    private Instant updatedAt;

    public void addCompletedStep(String step) {
        this.completedSteps.add(step);
        this.eventLog.add("COMPLETED: " + step);
        this.updatedAt = Instant.now();
    }

    public void addEvent(String event) {
        this.eventLog.add(event);
        this.updatedAt = Instant.now();
    }
}
