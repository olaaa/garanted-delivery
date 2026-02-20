package ru.olga.gof.pattern.saga.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import ru.olga.gof.pattern.common.model.SagaState;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Хранилище состояния саг (в памяти для демонстрации).
 *
 * В PRODUCTION:
 * - Использовать персистентное хранилище (PostgreSQL, MongoDB)
 * - Event Sourcing: хранить не состояние, а поток событий
 * - CQRS: отдельное чтение/запись
 *
 * ЗАЧЕМ ХРАНИТЬ СОСТОЯНИЕ:
 * - Идемпотентность: повторная обработка события не меняет результат
 * - Recovery: восстановление после сбоя оркестратора
 * - Компенсация: знаем, какие шаги откатить
 */
@Slf4j
@Repository
public class SagaStateRepository {

    // В production - база данных
    private final Map<String, SagaState> sagaStates = new ConcurrentHashMap<>();

    public void save(SagaState state) {
        log.info("Saving saga state: sagaId={}, status={}",
                 state.getSagaId(), state.getStatus());
        sagaStates.put(state.getSagaId(), state);
    }

    public Optional<SagaState> findBySagaId(String sagaId) {
        return Optional.ofNullable(sagaStates.get(sagaId));
    }

    public Optional<SagaState> findByOrderId(String orderId) {
        return sagaStates.values().stream()
                .filter(state -> state.getOrderId().equals(orderId))
                .findFirst();
    }

    /**
     * Проверка идемпотентности: уже обработали это событие?
     */
    public boolean isEventProcessed(String sagaId, String eventType) {
        return findBySagaId(sagaId)
                .map(state -> state.getEventLog().stream()
                        .anyMatch(log -> log.contains(eventType)))
                .orElse(false);
    }
}
