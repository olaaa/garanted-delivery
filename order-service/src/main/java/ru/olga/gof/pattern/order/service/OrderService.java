package ru.olga.gof.pattern.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.events.OrderCreatedEvent;
import ru.olga.gof.pattern.order.dto.CreateOrderRequest;
import ru.olga.gof.pattern.order.dto.CreateOrderResponse;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Сервис создания заказов с публикацией события.
 *
 * ВАЖНЫЙ ПАТТЕРН: Transactional Outbox (здесь упрощенно).
 * В production:
 * 1. Сохранить заказ в БД
 * 2. Сохранить событие в outbox таблицу (в той же транзакции)
 * 3. Отдельный процесс читает outbox и публикует в Pulsar
 * → Гарантия: либо оба действия, либо ни одного
 *
 * Здесь для простоты: сразу публикуем в Pulsar.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final PulsarClient pulsarClient;
    private final ObjectMapper objectMapper;

    private Producer<byte[]> orderCreatedProducer;

    @PostConstruct
    public void init() throws PulsarClientException {
        log.info("Initializing OrderCreatedEvent producer");

        orderCreatedProducer = pulsarClient.newProducer()
                .topic("persistent://public/default/order-created")
                .producerName("order-service-producer")
                .enableBatching(false)
                .sendTimeout(10, TimeUnit.SECONDS)
                .compressionType(CompressionType.LZ4)
                .create();

        log.info("✅ OrderCreatedEvent producer initialized");
    }

    /**
     * Создать заказ и опубликовать событие.
     */
    public CreateOrderResponse createOrder(CreateOrderRequest request) {
        // Генерация идентификаторов
        String orderId = "ORD-" + UUID.randomUUID();
        String sagaId = "SAGA-" + UUID.randomUUID();

        log.info("Creating order: orderId={}, sagaId={}", orderId, sagaId);

        // 1. Сохранить заказ в БД (здесь пропущено для краткости)
        // orderRepository.save(order);

        // 2. Создать событие
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .productCode(request.getProductCode())
                .quantity(request.getQuantity())
                .totalAmount(request.getTotalAmount())
                .idempotencyKey(orderId)
                .timestamp(Instant.now())
                .sagaId(sagaId)
                .build();

        // 3. Опубликовать событие в Pulsar
        try {
            byte[] payload = objectMapper.writeValueAsBytes(event);

            MessageId messageId = orderCreatedProducer.newMessage()
                    .key(event.getIdempotencyKey())
                    .value(payload)
                    .send();

            log.info("✅ OrderCreatedEvent published: orderId={}, messageId={}",
                     orderId, messageId);

        } catch (Exception e) {
            log.error("❌ Failed to publish OrderCreatedEvent for order {}", orderId, e);
            // В production: rollback БД транзакции или использовать outbox pattern
            throw new RuntimeException("Failed to publish order event", e);
        }

        // 4. Вернуть ответ клиенту
        return CreateOrderResponse.builder()
                .orderId(orderId)
                .sagaId(sagaId)
                .status("CREATED")
                .build();
    }

    @PreDestroy
    public void cleanup() {
        log.info("Closing OrderCreatedEvent producer");
        try {
            if (orderCreatedProducer != null) {
                orderCreatedProducer.close();
            }
        } catch (Exception e) {
            log.warn("Error closing producer", e);
        }
    }
}
