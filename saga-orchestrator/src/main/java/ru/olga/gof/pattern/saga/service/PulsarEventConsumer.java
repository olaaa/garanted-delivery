package ru.olga.gof.pattern.saga.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.events.*;
import ru.olga.gof.pattern.saga.config.TopicConfig;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * Consumer событий из Pulsar с гарантиями обработки.
 *
 * EXACTLY-ONCE ОБРАБОТКА:
 * 1. Получаем сообщение
 * 2. Проверяем идемпотентность (не обрабатывали ли уже)
 * 3. Обрабатываем
 * 4. Сохраняем состояние
 * 5. Acknowledge → Pulsar знает, что обработка завершена
 *
 * ВАЖНО:
 * - Acknowledge ПОСЛЕ успешной обработки (не до!)
 * - При ошибке: negativeAcknowledge → сообщение вернется в очередь
 * - Shared subscription: масштабирование обработки
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PulsarEventConsumer {

    private final PulsarClient pulsarClient;
    private final ObjectMapper objectMapper;
    private final SagaOrchestrator orchestrator;

    private Consumer<byte[]> orderCreatedConsumer;
    private Consumer<byte[]> paymentProcessedConsumer;
    private Consumer<byte[]> paymentFailedConsumer;
    private Consumer<byte[]> inventoryReservedConsumer;
    private Consumer<byte[]> inventoryFailedConsumer;

    /**
     * Запуск consumers после старта приложения.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startConsuming() {
        log.info("Starting Pulsar event consumers");

        try {
            // Consumer для событий создания заказа
            orderCreatedConsumer = createConsumer(
                    TopicConfig.ORDER_CREATED_TOPIC,
                    TopicConfig.SAGA_ORCHESTRATOR_SUBSCRIPTION
            );
            startConsumerThread(orderCreatedConsumer, this::handleOrderCreated);

            // Consumer для успешных платежей
            paymentProcessedConsumer = createConsumer(
                    TopicConfig.PAYMENT_PROCESSED_TOPIC,
                    TopicConfig.SAGA_ORCHESTRATOR_SUBSCRIPTION
            );
            startConsumerThread(paymentProcessedConsumer, this::handlePaymentProcessed);

            // Consumer для провалившихся платежей
            paymentFailedConsumer = createConsumer(
                    TopicConfig.PAYMENT_FAILED_TOPIC,
                    TopicConfig.SAGA_ORCHESTRATOR_SUBSCRIPTION
            );
            startConsumerThread(paymentFailedConsumer, this::handlePaymentFailed);

            // Consumer для успешных резервирований
            inventoryReservedConsumer = createConsumer(
                    TopicConfig.INVENTORY_RESERVED_TOPIC,
                    TopicConfig.SAGA_ORCHESTRATOR_SUBSCRIPTION
            );
            startConsumerThread(inventoryReservedConsumer, this::handleInventoryReserved);

            // Consumer для провалившихся резервирований
            inventoryFailedConsumer = createConsumer(
                    TopicConfig.INVENTORY_RESERVATION_FAILED_TOPIC,
                    TopicConfig.SAGA_ORCHESTRATOR_SUBSCRIPTION
            );
            startConsumerThread(inventoryFailedConsumer, this::handleInventoryFailed);

            log.info("✅ All event consumers started");

        } catch (Exception e) {
            log.error("Failed to start consumers", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Создание consumer с настройками для надежности.
     */
    private Consumer<byte[]> createConsumer(String topic, String subscription)
            throws PulsarClientException {

        return pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                // Shared subscription: несколько инстансов могут обрабатывать параллельно
                .subscriptionType(SubscriptionType.Shared)
                // Начинаем с самого раннего сообщения при первом подключении
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                // Ack timeout: если не acknowledge за 30 сек → redelivery
                .ackTimeout(30, TimeUnit.SECONDS)
                // Negative ack redelivery delay
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscribe();
    }

    /**
     * Запуск отдельного потока для consumer.
     */
    private void startConsumerThread(Consumer<byte[]> consumer,
                                     MessageHandler handler) {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = consumer.receive();
                    handleMessage(consumer, message, handler);
                } catch (Exception e) {
                    log.error("Error in consumer thread", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Обработка сообщения с гарантиями exactly-once.
     */
    private void handleMessage(Consumer<byte[]> consumer,
                               Message<byte[]> message,
                               MessageHandler handler) {
        try {
            log.debug("📨 Received message: key={}, messageId={}",
                      message.getKey(), message.getMessageId());

            // Обработка
            handler.handle(message);

            // Acknowledge ПОСЛЕ успешной обработки
            consumer.acknowledge(message);
            log.debug("✅ Message acknowledged: {}", message.getMessageId());

        } catch (Exception e) {
            log.error("❌ Error processing message {}: {}",
                      message.getMessageId(), e.getMessage(), e);

            // Negative acknowledge → сообщение вернется в очередь для повтора
            consumer.negativeAcknowledge(message);
            log.warn("🔄 Message negatively acknowledged for retry: {}",
                     message.getMessageId());
        }
    }

    private void handleOrderCreated(Message<byte[]> message) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(
                message.getValue(), OrderCreatedEvent.class);
        log.info("Processing OrderCreatedEvent: orderId={}", event.getOrderId());
        orchestrator.handleOrderCreated(event);
    }

    private void handlePaymentProcessed(Message<byte[]> message) throws Exception {
        PaymentProcessedEvent event = objectMapper.readValue(
                message.getValue(), PaymentProcessedEvent.class);
        log.info("Processing PaymentProcessedEvent: orderId={}", event.getOrderId());
        orchestrator.handlePaymentProcessed(event);
    }

    private void handlePaymentFailed(Message<byte[]> message) throws Exception {
        PaymentFailedEvent event = objectMapper.readValue(
                message.getValue(), PaymentFailedEvent.class);
        log.info("Processing PaymentFailedEvent: orderId={}", event.getOrderId());
        orchestrator.handlePaymentFailed(event);
    }

    private void handleInventoryReserved(Message<byte[]> message) throws Exception {
        InventoryReservedEvent event = objectMapper.readValue(
                message.getValue(), InventoryReservedEvent.class);
        log.info("Processing InventoryReservedEvent: orderId={}", event.getOrderId());
        orchestrator.handleInventoryReserved(event);
    }

    private void handleInventoryFailed(Message<byte[]> message) throws Exception {
        InventoryReservationFailedEvent event = objectMapper.readValue(
                message.getValue(), InventoryReservationFailedEvent.class);
        log.info("Processing InventoryReservationFailedEvent: orderId={}",
                 event.getOrderId());
        orchestrator.handleInventoryReservationFailed(event);
    }

    @PreDestroy
    public void cleanup() {
        log.info("Closing Pulsar consumers");
        closeQuietly(orderCreatedConsumer);
        closeQuietly(paymentProcessedConsumer);
        closeQuietly(paymentFailedConsumer);
        closeQuietly(inventoryReservedConsumer);
        closeQuietly(inventoryFailedConsumer);
    }

    private void closeQuietly(Consumer<?> consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
    }

    @FunctionalInterface
    interface MessageHandler {
        void handle(Message<byte[]> message) throws Exception;
    }
}
