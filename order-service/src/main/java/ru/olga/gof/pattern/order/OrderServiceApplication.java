package ru.olga.gof.pattern.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Order Service - сервис управления заказами.
 *
 * РОЛЬ В SAGA:
 * - Принимает запрос на создание заказа от клиента (REST API)
 * - Создает заказ в локальной БД
 * - Публикует событие OrderCreated в Pulsar
 * - НЕ знает о Payment/Inventory сервисах (loose coupling)
 *
 * ГАРАНТИИ ДОСТАВКИ:
 * - Transactional outbox pattern (можно добавить)
 * - Идемпотентность создания заказа
 * - Pulsar producer с retry
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
