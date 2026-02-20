package ru.olga.gof.pattern.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.commands.ProcessPaymentCommand;
import ru.olga.gof.pattern.common.commands.RefundPaymentCommand;
import ru.olga.gof.pattern.common.events.PaymentFailedEvent;
import ru.olga.gof.pattern.common.events.PaymentProcessedEvent;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Обработчик платежей с гарантиями идемпотентности.
 *
 * ИДЕМПОТЕНТНОСТЬ:
 * - Храним обработанные idempotencyKey
 * - Повторная команда → возвращаем тот же результат
 * - В production: хранить в БД, не в памяти
 *
 * EXACTLY-ONCE PROCESSING:
 * 1. Receive command
 * 2. Check idempotency (already processed?)
 * 3. Process payment (call external gateway)
 * 4. Store result
 * 5. Publish event
 * 6. Acknowledge message
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProcessor {

    private final PulsarClient pulsarClient;
    private final ObjectMapper objectMapper;

    // Хранилище обработанных платежей (в production - БД)
    private final Map<String, String> processedPayments = new ConcurrentHashMap<>();

    private Producer<byte[]> paymentProcessedProducer;
    private Producer<byte[]> paymentFailedProducer;
    private Consumer<byte[]> processPaymentConsumer;
    private Consumer<byte[]> refundPaymentConsumer;

    @PostConstruct
    public void init() throws PulsarClientException {
        log.info("Initializing Payment Service producers and consumers");

        // Producers для публикации событий
        paymentProcessedProducer = pulsarClient.newProducer()
                .topic("persistent://public/default/payment-processed")
                .producerName("payment-processed-producer")
                .enableBatching(false)
                .sendTimeout(10, TimeUnit.SECONDS)
                .create();

        paymentFailedProducer = pulsarClient.newProducer()
                .topic("persistent://public/default/payment-failed")
                .producerName("payment-failed-producer")
                .enableBatching(false)
                .sendTimeout(10, TimeUnit.SECONDS)
                .create();

        // Consumers для получения команд
        processPaymentConsumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/process-payment")
                .subscriptionName("payment-service-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(30, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscribe();

        refundPaymentConsumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/refund-payment")
                .subscriptionName("payment-service-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(30, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscribe();

        // Запуск consumer threads
        startProcessPaymentConsumer();
        startRefundPaymentConsumer();

        log.info("✅ Payment Service initialized");
    }

    private void startProcessPaymentConsumer() {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = processPaymentConsumer.receive();
                    handleProcessPayment(message);
                } catch (Exception e) {
                    log.error("Error in processPayment consumer", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void startRefundPaymentConsumer() {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = refundPaymentConsumer.receive();
                    handleRefundPayment(message);
                } catch (Exception e) {
                    log.error("Error in refundPayment consumer", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Обработка команды ProcessPayment.
     */
    private void handleProcessPayment(Message<byte[]> message) {
        try {
            ProcessPaymentCommand command = objectMapper.readValue(
                    message.getValue(), ProcessPaymentCommand.class);

            log.info("📨 Processing payment: orderId={}, amount={}, idempotencyKey={}",
                     command.getOrderId(), command.getAmount(), command.getIdempotencyKey());

            // ИДЕМПОТЕНТНОСТЬ: уже обработали?
            if (processedPayments.containsKey(command.getIdempotencyKey())) {
                log.warn("⚠️ Payment already processed: {}", command.getIdempotencyKey());
                processPaymentConsumer.acknowledge(message);
                return;
            }

            // Симуляция обработки платежа
            boolean success = processPayment(command);

            if (success) {
                String paymentId = "PAY-" + UUID.randomUUID();

                // Сохраняем результат (идемпотентность)
                processedPayments.put(command.getIdempotencyKey(), paymentId);

                // Публикуем событие успеха
                PaymentProcessedEvent event = PaymentProcessedEvent.builder()
                        .orderId(command.getOrderId())
                        .sagaId(command.getSagaId())
                        .paymentId(paymentId)
                        .amount(command.getAmount())
                        .customerId(command.getCustomerId())
                        .idempotencyKey(command.getIdempotencyKey())
                        .timestamp(Instant.now())
                        .transactionId("TXN-" + UUID.randomUUID())
                        .paymentMethod("CREDIT_CARD")
                        .build();

                publishPaymentProcessed(event);

                log.info("✅ Payment processed: orderId={}, paymentId={}",
                         command.getOrderId(), paymentId);
            } else {
                // Публикуем событие ошибки
                PaymentFailedEvent event = PaymentFailedEvent.builder()
                        .orderId(command.getOrderId())
                        .sagaId(command.getSagaId())
                        .customerId(command.getCustomerId())
                        .reason("Insufficient funds")
                        .idempotencyKey(command.getIdempotencyKey())
                        .timestamp(Instant.now())
                        .build();

                publishPaymentFailed(event);

                log.warn("❌ Payment failed: orderId={}", command.getOrderId());
            }

            // Acknowledge ПОСЛЕ успешной обработки
            processPaymentConsumer.acknowledge(message);

        } catch (Exception e) {
            log.error("Error processing payment command", e);
            processPaymentConsumer.negativeAcknowledge(message);
        }
    }

    /**
     * COMPENSATING ACTION: возврат платежа.
     */
    private void handleRefundPayment(Message<byte[]> message) {
        try {
            RefundPaymentCommand command = objectMapper.readValue(
                    message.getValue(), RefundPaymentCommand.class);

            log.info("🔄 Refunding payment: orderId={}, paymentId={}, reason={}",
                     command.getOrderId(), command.getPaymentId(), command.getReason());

            // ИДЕМПОТЕНТНОСТЬ
            String refundKey = command.getIdempotencyKey() + "-refund";
            if (processedPayments.containsKey(refundKey)) {
                log.warn("⚠️ Refund already processed: {}", refundKey);
                refundPaymentConsumer.acknowledge(message);
                return;
            }

            // Симуляция возврата денег
            boolean refunded = refundPayment(command);

            if (refunded) {
                processedPayments.put(refundKey, "REFUNDED");
                log.info("✅ Payment refunded: orderId={}, paymentId={}",
                         command.getOrderId(), command.getPaymentId());
            } else {
                log.error("❌ Refund failed: orderId={}", command.getOrderId());
                // В production: manual intervention, alerts
            }

            refundPaymentConsumer.acknowledge(message);

        } catch (Exception e) {
            log.error("Error processing refund command", e);
            refundPaymentConsumer.negativeAcknowledge(message);
        }
    }

    /**
     * Симуляция обработки платежа (вызов внешнего payment gateway).
     * В production: интеграция с Stripe, PayPal и т.д.
     */
    private boolean processPayment(ProcessPaymentCommand command) {
        try {
            // Симуляция задержки
            Thread.sleep(100);

            // Симуляция: 90% успеха, 10% ошибки
            return Math.random() > 0.1;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Симуляция возврата платежа.
     */
    private boolean refundPayment(RefundPaymentCommand command) {
        try {
            Thread.sleep(100);
            return true; // Возвраты обычно успешны
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void publishPaymentProcessed(PaymentProcessedEvent event) throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(event);
        paymentProcessedProducer.newMessage()
                .key(event.getIdempotencyKey())
                .value(payload)
                .send();
    }

    private void publishPaymentFailed(PaymentFailedEvent event) throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(event);
        paymentFailedProducer.newMessage()
                .key(event.getIdempotencyKey())
                .value(payload)
                .send();
    }

    @PreDestroy
    public void cleanup() {
        log.info("Closing Payment Service resources");
        closeQuietly(paymentProcessedProducer);
        closeQuietly(paymentFailedProducer);
        closeQuietly(processPaymentConsumer);
        closeQuietly(refundPaymentConsumer);
    }

    private void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            log.warn("Error closing resource", e);
        }
    }
}
