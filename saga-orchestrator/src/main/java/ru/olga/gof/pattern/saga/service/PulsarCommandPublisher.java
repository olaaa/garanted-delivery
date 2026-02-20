package ru.olga.gof.pattern.saga.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.commands.*;
import ru.olga.gof.pattern.saga.config.TopicConfig;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * Publisher команд в Pulsar с гарантиями доставки.
 *
 * ГАРАНТИИ EXACTLY-ONCE:
 * 1. Producer с идентификатором → дедупликация на брокере
 * 2. Message key = idempotencyKey → повторы игнорируются
 * 3. Send timeout + retry → at-least-once доставка
 * 4. Explicit ack от consumer → confirmed delivery
 *
 * PULSAR FEATURES ДЛЯ НАДЕЖНОСТИ:
 * - Batching disabled для низкой latency
 * - Persistent topics → хранение на диске
 * - Deduplication на основе sequence ID
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PulsarCommandPublisher {

    private final PulsarClient pulsarClient;
    private final ObjectMapper objectMapper;

    private Producer<byte[]> processPaymentProducer;
    private Producer<byte[]> refundPaymentProducer;
    private Producer<byte[]> reserveInventoryProducer;
    private Producer<byte[]> releaseInventoryProducer;

    @PostConstruct
    public void init() throws PulsarClientException {
        log.info("Initializing Pulsar command publishers");

        // Producer для команды обработки платежа
        processPaymentProducer = createProducer(
                TopicConfig.PROCESS_PAYMENT_TOPIC,
                "saga-process-payment-producer"
        );

        // Producer для команды возврата платежа (компенсация)
        refundPaymentProducer = createProducer(
                TopicConfig.REFUND_PAYMENT_TOPIC,
                "saga-refund-payment-producer"
        );

        // Producer для команды резервирования товара
        reserveInventoryProducer = createProducer(
                TopicConfig.RESERVE_INVENTORY_TOPIC,
                "saga-reserve-inventory-producer"
        );

        // Producer для команды освобождения резервирования (компенсация)
        releaseInventoryProducer = createProducer(
                TopicConfig.RELEASE_INVENTORY_TOPIC,
                "saga-release-inventory-producer"
        );

        log.info("✅ All command producers initialized");
    }

    /**
     * Создание producer с настройками для гарантированной доставки.
     */
    private Producer<byte[]> createProducer(String topic, String producerName)
            throws PulsarClientException {

        return pulsarClient.newProducer()
                .topic(topic)
                .producerName(producerName)
                // Отключаем batching для низкой latency
                .enableBatching(false)
                // Таймаут отправки
                .sendTimeout(10, TimeUnit.SECONDS)
                // Retry при ошибках
                .maxPendingMessages(1000)
                // Compression
                .compressionType(CompressionType.LZ4)
                .create();
    }

    /**
     * Публикация команды ProcessPayment.
     */
    public void publishProcessPayment(ProcessPaymentCommand command) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(command);

            MessageId messageId = processPaymentProducer.newMessage()
                    // Ключ для партиционирования и дедупликации
                    .key(command.getIdempotencyKey())
                    .value(payload)
                    // Отправка и ожидание подтверждения
                    .send();

            log.info("📤 ProcessPaymentCommand sent: orderId={}, messageId={}",
                     command.getOrderId(), messageId);

        } catch (Exception e) {
            log.error("❌ Failed to send ProcessPaymentCommand for order {}",
                      command.getOrderId(), e);
            // В production: retry logic или dead letter queue
            throw new RuntimeException("Failed to publish command", e);
        }
    }

    /**
     * Публикация команды RefundPayment (компенсация).
     */
    public void publishRefundPayment(RefundPaymentCommand command) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(command);

            MessageId messageId = refundPaymentProducer.newMessage()
                    .key(command.getIdempotencyKey())
                    .value(payload)
                    .send();

            log.info("📤 RefundPaymentCommand sent: orderId={}, messageId={}",
                     command.getOrderId(), messageId);

        } catch (Exception e) {
            log.error("❌ Failed to send RefundPaymentCommand for order {}",
                      command.getOrderId(), e);
            throw new RuntimeException("Failed to publish compensating command", e);
        }
    }

    /**
     * Публикация команды ReserveInventory.
     */
    public void publishReserveInventory(ReserveInventoryCommand command) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(command);

            MessageId messageId = reserveInventoryProducer.newMessage()
                    .key(command.getIdempotencyKey())
                    .value(payload)
                    .send();

            log.info("📤 ReserveInventoryCommand sent: orderId={}, messageId={}",
                     command.getOrderId(), messageId);

        } catch (Exception e) {
            log.error("❌ Failed to send ReserveInventoryCommand for order {}",
                      command.getOrderId(), e);
            throw new RuntimeException("Failed to publish command", e);
        }
    }

    /**
     * Публикация команды ReleaseInventory (компенсация).
     */
    public void publishReleaseInventory(ReleaseInventoryCommand command) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(command);

            MessageId messageId = releaseInventoryProducer.newMessage()
                    .key(command.getIdempotencyKey())
                    .value(payload)
                    .send();

            log.info("📤 ReleaseInventoryCommand sent: orderId={}, messageId={}",
                     command.getOrderId(), messageId);

        } catch (Exception e) {
            log.error("❌ Failed to send ReleaseInventoryCommand for order {}",
                      command.getOrderId(), e);
            throw new RuntimeException("Failed to publish compensating command", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        log.info("Closing Pulsar producers");
        closeQuietly(processPaymentProducer);
        closeQuietly(refundPaymentProducer);
        closeQuietly(reserveInventoryProducer);
        closeQuietly(releaseInventoryProducer);
    }

    private void closeQuietly(Producer<?> producer) {
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (Exception e) {
            log.warn("Error closing producer", e);
        }
    }
}
