package ru.olga.gof.pattern.saga.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.commands.*;
import ru.olga.gof.pattern.common.events.*;
import ru.olga.gof.pattern.common.model.SagaState;
import ru.olga.gof.pattern.common.model.SagaStatus;
import ru.olga.gof.pattern.saga.repository.SagaStateRepository;

import java.time.Instant;

/**
 * Saga Orchestrator - мозг распределенной транзакции.
 *
 * ПАТТЕРН ORCHESTRATION:
 * Оркестратор централизованно управляет потоком выполнения:
 * 1. Получает событие OrderCreated
 * 2. Отправляет команду ProcessPayment
 * 3. Получает событие PaymentProcessed
 * 4. Отправляет команду ReserveInventory
 * 5. Получает событие InventoryReserved
 * 6. Завершает сагу
 *
 * При ошибке на любом шаге:
 * 1. Получает *Failed событие
 * 2. Определяет успешные шаги из SagaState
 * 3. Отправляет compensating commands в обратном порядке
 *
 * ПРЕИМУЩЕСТВА ORCHESTRATION:
 * + Централизованная логика (легко понять flow)
 * + Явное управление компенсациями
 * + Легко добавить новые шаги
 *
 * НЕДОСТАТКИ:
 * - Single point of failure (нужна отказоустойчивость)
 * - Coupling: оркестратор знает о всех участниках
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final SagaStateRepository stateRepository;
    private final PulsarCommandPublisher commandPublisher;

    /**
     * Шаг 1: Начало саги после создания заказа.
     */
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Starting saga for order: {}", event.getOrderId());

        // Идемпотентность: проверяем, не обрабатывали ли уже
        if (stateRepository.findByOrderId(event.getOrderId()).isPresent()) {
            log.warn("Saga for order {} already exists, skipping", event.getOrderId());
            return;
        }

        // Создаем состояние саги (Event Sourcing элемент)
        SagaState state = SagaState.builder()
                .sagaId(event.getSagaId())
                .orderId(event.getOrderId())
                .status(SagaStatus.STARTED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        state.addEvent("OrderCreated: " + event.getOrderId());
        stateRepository.save(state);

        // Отправляем команду на обработку платежа
        ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                .orderId(event.getOrderId())
                .sagaId(event.getSagaId())
                .customerId(event.getCustomerId())
                .amount(event.getTotalAmount())
                .idempotencyKey(event.getOrderId() + "-payment")
                .timestamp(Instant.now())
                .build();

        commandPublisher.publishProcessPayment(command);

        state.setStatus(SagaStatus.PAYMENT_PROCESSING);
        state.addEvent("ProcessPaymentCommand sent");
        stateRepository.save(state);

    }

    /**
     * Шаг 2: Платеж обработан успешно → резервируем товар.
     */
    public void handlePaymentProcessed(PaymentProcessedEvent paymentProcessedEvent) {
        log.info("Payment processed for order: {}", paymentProcessedEvent.getOrderId());

        SagaState sagaState = stateRepository.findBySagaId(paymentProcessedEvent.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + paymentProcessedEvent.getSagaId()));

        // Идемпотентность: уже обработали?
        if (stateRepository.isEventProcessed(paymentProcessedEvent.getSagaId(), "PaymentProcessed")) {
            log.warn("PaymentProcessed already handled for saga {}", paymentProcessedEvent.getSagaId());
            return;
        }

        sagaState.setStatus(SagaStatus.PAYMENT_COMPLETED);
        sagaState.addCompletedStep("Payment: " + paymentProcessedEvent.getPaymentId());
        sagaState.addEvent("PaymentProcessed: " + paymentProcessedEvent.getPaymentId());
        stateRepository.save(sagaState);

        // Отправляем команду на резервирование товара
        // Примечание: в реальном проекте данные о товаре нужно получить из заказа
        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .orderId(paymentProcessedEvent.getOrderId())
                .sagaId(paymentProcessedEvent.getSagaId())
                .productCode("PRODUCT-001") // В production - из заказа
                .quantity(1)
                .idempotencyKey(paymentProcessedEvent.getOrderId() + "-inventory")
                .timestamp(Instant.now())
                .build();

        commandPublisher.publishReserveInventory(command);

        sagaState.setStatus(SagaStatus.INVENTORY_RESERVING);
        sagaState.addEvent("ReserveInventoryCommand sent");
        stateRepository.save(sagaState);
    }

    /**
     * Шаг 3: Товар зарезервирован → сага завершена успешно.
     */
    public void handleInventoryReserved(InventoryReservedEvent event) {
        log.info("Inventory reserved for order: {}", event.getOrderId());

        SagaState state = stateRepository.findBySagaId(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + event.getSagaId()));

        if (stateRepository.isEventProcessed(event.getSagaId(), "InventoryReserved")) {
            log.warn("InventoryReserved already handled for saga {}", event.getSagaId());
            return;
        }

        state.setStatus(SagaStatus.COMPLETED);
        state.addCompletedStep("Inventory: " + event.getReservationId());
        state.addEvent("InventoryReserved: " + event.getReservationId());
        stateRepository.save(state);

        log.info("✅ Saga completed successfully: {}", event.getSagaId());
    }

    /**
     * КОМПЕНСАЦИЯ: Платеж провалился → отменяем заказ.
     */
    public void handlePaymentFailed(PaymentFailedEvent event) {
        log.error("Payment failed for order {}: {}", event.getOrderId(), event.getReason());

        SagaState state = stateRepository.findBySagaId(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + event.getSagaId()));

        state.setStatus(SagaStatus.COMPENSATING);
        state.addEvent("PaymentFailed: " + event.getReason());
        stateRepository.save(state);

        // Платеж не прошел → нечего компенсировать
        state.setStatus(SagaStatus.COMPENSATED);
        stateRepository.save(state);

        log.info("🔄 Saga compensated (no steps to rollback): {}", event.getSagaId());
    }

    /**
     * КОМПЕНСАЦИЯ: Резервирование провалилось → возвращаем деньги.
     */
    public void handleInventoryReservationFailed(InventoryReservationFailedEvent inventoryReservationFailedEvent) {
        log.error("Inventory reservation failed for order {}: {}",
                  inventoryReservationFailedEvent.getOrderId(), inventoryReservationFailedEvent.getReason());

        SagaState state = stateRepository.findBySagaId(inventoryReservationFailedEvent.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + inventoryReservationFailedEvent.getSagaId()));

        state.setStatus(SagaStatus.COMPENSATING);
        state.addEvent("InventoryReservationFailed: " + inventoryReservationFailedEvent.getReason());
        stateRepository.save(state);

        // Откатываем успешные шаги в обратном порядке
        if (state.getCompletedSteps().stream().anyMatch(s -> s.startsWith("Payment"))) {
            log.info("🔄 Compensating payment for saga: {}", inventoryReservationFailedEvent.getSagaId());

            // Извлекаем paymentId из completedSteps
            String paymentId = extractPaymentId(state);

            RefundPaymentCommand publishRefundPayment = RefundPaymentCommand.builder()
                    .orderId(inventoryReservationFailedEvent.getOrderId())
                    .sagaId(inventoryReservationFailedEvent.getSagaId())
                    .paymentId(paymentId)
                    .amount(null) // В production - из заказа
                    .reason("Inventory not available")
                    .idempotencyKey(inventoryReservationFailedEvent.getOrderId() + "-refund")
                    .timestamp(Instant.now())
                    .build();

            commandPublisher.publishRefundPayment(publishRefundPayment);
        }

        state.setStatus(SagaStatus.COMPENSATED);
        stateRepository.save(state);

        log.info("🔄 Saga compensated: {}", inventoryReservationFailedEvent.getSagaId());
    }

    private String extractPaymentId(SagaState state) {
        return state.getCompletedSteps().stream()
                .filter(s -> s.startsWith("Payment:"))
                .map(s -> s.substring("Payment:".length()).trim())
                .findFirst()
                .orElse("unknown");
    }
}
