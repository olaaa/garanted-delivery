package ru.olga.gof.pattern.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Service;
import ru.olga.gof.pattern.common.commands.ReleaseInventoryCommand;
import ru.olga.gof.pattern.common.commands.ReserveInventoryCommand;
import ru.olga.gof.pattern.common.events.InventoryReservationFailedEvent;
import ru.olga.gof.pattern.common.events.InventoryReservedEvent;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Управление складскими запасами с резервированием.
 *
 * ПАТТЕРН PESSIMISTIC LOCKING:
 * - Резервируем товар сразу (блокируем количество)
 * - Если сага откатывается → освобождаем резервирование
 * - Альтернатива: оптимистичная блокировка (проверка при финализации)
 *
 * ИДЕМПОТЕНТНОСТЬ:
 * - Храним обработанные резервирования
 * - Повторная команда → возвращаем тот же результат
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryManager {

    private final PulsarClient pulsarClient;
    private final ObjectMapper objectMapper;

    // Текущие запасы (в production - БД)
    private final Map<String, Integer> stock = new ConcurrentHashMap<>();

    // Обработанные резервирования (идемпотентность)
    private final Map<String, String> processedReservations = new ConcurrentHashMap<>();

    private Producer<byte[]> inventoryReservedProducer;
    private Producer<byte[]> inventoryFailedProducer;
    private Consumer<byte[]> reserveInventoryConsumer;
    private Consumer<byte[]> releaseInventoryConsumer;

    @PostConstruct
    public void init() throws PulsarClientException {
        log.info("Initializing Inventory Service");

        // Инициализация начальных запасов
        stock.put("PRODUCT-001", 100);
        stock.put("PRODUCT-002", 50);
        log.info("Initial stock loaded: {}", stock);

        // Producers
        inventoryReservedProducer = pulsarClient.newProducer()
                .topic("persistent://public/default/inventory-reserved")
                .producerName("inventory-reserved-producer")
                .enableBatching(false)
                .sendTimeout(10, TimeUnit.SECONDS)
                .create();

        inventoryFailedProducer = pulsarClient.newProducer()
                .topic("persistent://public/default/inventory-reservation-failed")
                .producerName("inventory-failed-producer")
                .enableBatching(false)
                .sendTimeout(10, TimeUnit.SECONDS)
                .create();

        // Consumers
        reserveInventoryConsumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/reserve-inventory")
                .subscriptionName("inventory-service-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(30, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscribe();

        releaseInventoryConsumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/release-inventory")
                .subscriptionName("inventory-service-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(30, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscribe();

        // Запуск consumer threads
        startReserveInventoryConsumer();
        startReleaseInventoryConsumer();

        log.info("✅ Inventory Service initialized");
    }

    private void startReserveInventoryConsumer() {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = reserveInventoryConsumer.receive();
                    handleReserveInventory(message);
                } catch (Exception e) {
                    log.error("Error in reserveInventory consumer", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void startReleaseInventoryConsumer() {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = releaseInventoryConsumer.receive();
                    handleReleaseInventory(message);
                } catch (Exception e) {
                    log.error("Error in releaseInventory consumer", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Обработка команды резервирования товара.
     */
    private void handleReserveInventory(Message<byte[]> message) {
        try {
            ReserveInventoryCommand command = objectMapper.readValue(
                    message.getValue(), ReserveInventoryCommand.class);

            log.info("📨 Reserving inventory: orderId={}, product={}, quantity={}, idempotencyKey={}",
                     command.getOrderId(), command.getProductCode(),
                     command.getQuantity(), command.getIdempotencyKey());

            // ИДЕМПОТЕНТНОСТЬ
            if (processedReservations.containsKey(command.getIdempotencyKey())) {
                log.warn("⚠️ Reservation already processed: {}", command.getIdempotencyKey());
                reserveInventoryConsumer.acknowledge(message);
                return;
            }

            // Проверка наличия товара
            Integer available = stock.getOrDefault(command.getProductCode(), 0);

            if (available >= command.getQuantity()) {
                // Резервируем (уменьшаем запас)
                stock.put(command.getProductCode(), available - command.getQuantity());

                String reservationId = "RES-" + UUID.randomUUID();
                processedReservations.put(command.getIdempotencyKey(), reservationId);

                // Публикуем событие успеха
                InventoryReservedEvent event = InventoryReservedEvent.builder()
                        .orderId(command.getOrderId())
                        .sagaId(command.getSagaId())
                        .productCode(command.getProductCode())
                        .quantity(command.getQuantity())
                        .reservationId(reservationId)
                        .idempotencyKey(command.getIdempotencyKey())
                        .timestamp(Instant.now())
                        .build();

                publishInventoryReserved(event);

                log.info("✅ Inventory reserved: orderId={}, reservationId={}, remaining={}",
                         command.getOrderId(), reservationId,
                         stock.get(command.getProductCode()));

            } else {
                // Недостаточно товара
                InventoryReservationFailedEvent event = InventoryReservationFailedEvent.builder()
                        .orderId(command.getOrderId())
                        .sagaId(command.getSagaId())
                        .productCode(command.getProductCode())
                        .requestedQuantity(command.getQuantity())
                        .availableQuantity(available)
                        .reason("Insufficient stock")
                        .idempotencyKey(command.getIdempotencyKey())
                        .timestamp(Instant.now())
                        .build();

                publishInventoryFailed(event);

                log.warn("❌ Inventory reservation failed: orderId={}, requested={}, available={}",
                         command.getOrderId(), command.getQuantity(), available);
            }

            reserveInventoryConsumer.acknowledge(message);

        } catch (Exception e) {
            log.error("Error processing reserve inventory command", e);
            reserveInventoryConsumer.negativeAcknowledge(message);
        }
    }

    /**
     * COMPENSATING ACTION: освобождение резервирования.
     */
    private void handleReleaseInventory(Message<byte[]> message) {
        try {
            ReleaseInventoryCommand command = objectMapper.readValue(
                    message.getValue(), ReleaseInventoryCommand.class);

            log.info("🔄 Releasing inventory: orderId={}, reservationId={}, reason={}",
                     command.getOrderId(), command.getReservationId(), command.getReason());

            // ИДЕМПОТЕНТНОСТЬ
            String releaseKey = command.getIdempotencyKey() + "-release";
            if (processedReservations.containsKey(releaseKey)) {
                log.warn("⚠️ Release already processed: {}", releaseKey);
                releaseInventoryConsumer.acknowledge(message);
                return;
            }

            // Возвращаем товар на склад
            Integer current = stock.getOrDefault(command.getProductCode(), 0);
            stock.put(command.getProductCode(), current + command.getQuantity());

            processedReservations.put(releaseKey, "RELEASED");

            log.info("✅ Inventory released: orderId={}, product={}, newStock={}",
                     command.getOrderId(), command.getProductCode(),
                     stock.get(command.getProductCode()));

            releaseInventoryConsumer.acknowledge(message);

        } catch (Exception e) {
            log.error("Error processing release inventory command", e);
            releaseInventoryConsumer.negativeAcknowledge(message);
        }
    }

    private void publishInventoryReserved(InventoryReservedEvent event) throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(event);
        inventoryReservedProducer.newMessage()
                .key(event.getIdempotencyKey())
                .value(payload)
                .send();
    }

    private void publishInventoryFailed(InventoryReservationFailedEvent event) throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(event);
        inventoryFailedProducer.newMessage()
                .key(event.getIdempotencyKey())
                .value(payload)
                .send();
    }

    @PreDestroy
    public void cleanup() {
        log.info("Closing Inventory Service resources");
        closeQuietly(inventoryReservedProducer);
        closeQuietly(inventoryFailedProducer);
        closeQuietly(reserveInventoryConsumer);
        closeQuietly(releaseInventoryConsumer);
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
