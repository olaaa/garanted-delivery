# Быстрый старт

## За 5 минут запустить и протестировать проект

### 1. Запустить Pulsar

```bash
docker compose up pulsar -d
```

Дождаться готовности:
```bash
docker compose logs -f pulsar | grep "messaging service is ready"
```

### 2. Собрать проект

```bash
mvn clean install -DskipTests
```

### 3. Запустить сервисы

**В IntelliJ IDEA:**
- Открыть проект
- Запустить (Run) каждый Application класс:
  1. `saga-orchestrator/src/.../SagaOrchestratorApplication.java`
  2. `order-service/src/.../OrderServiceApplication.java`
  3. `payment-service/src/.../PaymentServiceApplication.java`
  4. `inventory-service/src/.../InventoryServiceApplication.java`

**Или из командной строки:**
```bash
# Terminal 1
java -jar saga-orchestrator/target/saga-orchestrator-1.0-SNAPSHOT.jar

# Terminal 2
java -jar order-service/target/order-service-1.0-SNAPSHOT.jar

# Terminal 3
java -jar payment-service/target/payment-service-1.0-SNAPSHOT.jar

# Terminal 4
java -jar inventory-service/target/inventory-service-1.0-SNAPSHOT.jar
```

### 4. Тестировать

**Успешный заказ:**
```bash
chmod +x test-successful-order.sh
./test-successful-order.sh
```

**Заказ с компенсацией:**
```bash
chmod +x test-compensation.sh
./test-compensation.sh
```

**Или вручную:**
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productCode": "PRODUCT-001",
    "quantity": 2,
    "totalAmount": 199.99
  }'
```

### 5. Проверить логи

**Успешная сага:**
```
[saga-orchestrator] OrderCreated received
[saga-orchestrator] ProcessPaymentCommand sent
[payment-service] Payment processed: orderId=ORD-xxx
[saga-orchestrator] PaymentProcessed received
[saga-orchestrator] ReserveInventoryCommand sent
[inventory-service] Inventory reserved: orderId=ORD-xxx
[saga-orchestrator] InventoryReserved received
[saga-orchestrator] ✅ Saga completed successfully
```

**Сага с компенсацией:**
```
[saga-orchestrator] OrderCreated received
[saga-orchestrator] ProcessPaymentCommand sent
[payment-service] ✅ Payment processed
[saga-orchestrator] PaymentProcessed received
[saga-orchestrator] ReserveInventoryCommand sent
[inventory-service] ❌ Insufficient stock
[saga-orchestrator] InventoryReservationFailed received
[saga-orchestrator] 🔄 Saga COMPENSATING
[saga-orchestrator] RefundPaymentCommand sent
[payment-service] 🔄 Refunding payment
[saga-orchestrator] ✅ Saga compensated
```

### 6. Остановить

```bash
# Остановить сервисы (Ctrl+C в каждом терминале)

# Остановить Pulsar
docker-compose down
```

---

## Что дальше?

- Прочитать [README.md](README.md) для понимания архитектуры
- Прочитать [ARCHITECTURE.md](ARCHITECTURE.md) для деталей реализации
- Изучить код с комментариями
- Поэкспериментировать:
  - Заказать больше товара, чем есть на складе
  - Симулировать сбой (остановить сервис)
  - Добавить новый шаг в сагу

## Troubleshooting

**Pulsar не стартует:**
```bash
docker-compose logs pulsar
```

**Сервисы не подключаются к Pulsar:**
- Проверить, что Pulsar запущен: `docker ps`
- Проверить порт 6650: `netstat -an | grep 6650`

**Сообщения не обрабатываются:**
- Проверить топики: `docker exec -it pulsar bin/pulsar-admin topics list public/default`
- Проверить subscriptions: `docker exec -it pulsar bin/pulsar-admin topics stats persistent://public/default/order-created`

**Изменить Pulsar URL:**
```bash
# В application.yml каждого сервиса
pulsar:
  service-url: pulsar://localhost:6650
```
