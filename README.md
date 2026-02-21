# Guaranteed Delivery & Saga Pattern Demo

Учебный проект, демонстрирующий **гарантированную доставку сообщений** и **откат распределенных транзакций** в микросервисной архитектуре с использованием **Apache Pulsar**.

## 🎯 Цели проекта

Наглядно продемонстрировать:

1. **Гарантированную доставку** (at-least-once / exactly-once semantics)
2. **Saga Pattern** для управления распределенными транзакциями
3. **Compensating Transactions** для отката изменений при ошибках
4. **Event Sourcing** элементы для отслеживания состояния
5. **Idempotency** для exactly-once обработки

## 🏗️ Архитектура

### Микросервисы

- **Order Service** (порт 8081): Создание заказов, публикация OrderCreated
- **Saga Orchestrator** (порт 8080): Координация распределенной транзакции
- **Payment Service** (порт 8082): Обработка платежей и возвратов
- **Inventory Service** (порт 8083): Резервирование товаров на складе

### Выбор паттерна: **Saga Orchestration**

**Почему Orchestration, а не Choreography?**

✅ **Преимущества для учебного проекта:**
- **Централизованная логика**: легко понять весь flow в одном месте (SagaOrchestrator)
- **Явное управление**: оркестратор явно отправляет команды и обрабатывает ответы
- **Простота отладки**: весь state машины в одном сервисе
- **Легкость компенсаций**: оркестратор знает, какие шаги откатить

❌ **Недостатки:**
- Single point of failure (требуется HA для production)
- Coupling: оркестратор знает о всех участниках

**Choreography** был бы лучше для:
- Высокой decoupling (каждый сервис независим)
- Масштабируемости (нет центрального узла)
- Но сложнее понять flow и отлаживать

## 📊 Диаграмма последовательности

### Успешный сценарий

[Mermaid](успешный%20сценарий.mermaid)

### Сценарий с компенсацией

[Mermaid](Сценарий%20с%20компенсацией.mermaid)

1. [ ] **TODO** разобраться в том, как изменится состояние Заказ при компенсации. У заказа должно быть поле состояния какое-то

## 🔐 Гарантии доставки

### 1. **At-Least-Once Delivery**

**Механизм:**
- Pulsar не удаляет сообщение до получения `acknowledge()`
- При сбое consumer → сообщение автоматически redelivered
- `ackTimeout(30s)`: если за 30 сек нет ack → повтор

**Код:**
```java
consumer.acknowledge(message); // ПОСЛЕ успешной обработки
consumer.negativeAcknowledge(message); // При ошибке → retry
```

### 2. **Exactly-Once Semantics**

**Проблема:** At-least-once может привести к дублям (сетевая ошибка после обработки, но до ack).

**Решение - Idempotency:**
Каждое событие/команда содержит `idempotencyKey`. 

* Ключ идемпотентности генерируется в начале процесса, а именно в `OrderService`:  
```java
OrderCreatedEvent event = OrderCreatedEvent.builder()
    .orderId(orderId)
    .idempotencyKey(orderId)  // ← Ключ для дедупликации
    .build();
```
* В `PulsarCommandPublisher` копируется ключ идемпотентности в каждую из команд:

```java
command.setIdempotencyKey(event.getIdempotencyKey());
```

* Каждый Consumer проверяет перед обработкой -- `PaysService`, `InventoryService`:
```java
if (processedPayments.containsKey(command.getIdempotencyKey())) {
    log.warn("Already processed, skipping");
    consumer.acknowledge(message);
    return;
}
```
```java
// 3. Обрабатываем
processPayment(command);
```
* Сохраняем факт обработки. Успех:
```java
 processedPayments.put(command.getIdempotencyKey(), paymentId);
```
* Сохраняем факт обработки. Возврат:
```java

String refundKey = command.getIdempotencyKey() + "-refund";
processedPayments.put(refundKey, "REFUNDED");
```
```java
// 5. Acknowledge
consumer.acknowledge(message);
```

**Pulsar features для exactly-once:**
- **Deduplication**: встроенная дедупликация на уровне брокера
- **Transactions**: атомарная отправка/получение нескольких сообщений
- **Message key**: партиционирование и дедупликация по ключу

### 3. **Producer Reliability**
Reliability -- надёжность

```java
Producer<byte[]> producer = pulsarClient.newProducer()
    .topic(topic)
    .enableBatching(false)        // Низкая latency!!! На практике лучше включить!!!!
    .sendTimeout(10, TimeUnit.SECONDS)  // Retry при ошибках
    .compressionType(CompressionType.LZ4)
    .create();

// Синхронная отправка (ждем подтверждения)
MessageId messageId = producer.newMessage()
    .key(idempotencyKey)
    .value(payload)
    .send();  // Блокируется до подтверждения брокером
```

### 4. **Consumer Reliability**

```java
Consumer<byte[]> consumer = pulsarClient.newConsumer()
    .topic(topic)
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.Shared)  // Масштабирование
    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    .ackTimeout(30, TimeUnit.SECONDS)  // Redelivery при таймауте
    .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
    .subscribe();
```

## 🔄 Compensating Transactions

### Принцип

Каждый шаг саги имеет **компенсирующее действие**:

| Действие | Компенсация |
|----------|-------------|
| ProcessPayment → PaymentProcessed | RefundPayment |
| ReserveInventory → InventoryReserved | ReleaseInventory |

### Логика компенсации

```java
// Оркестратор хранит состояние: какие шаги выполнены
SagaState state = SagaState.builder()
    .sagaId(sagaId)
    .completedSteps(new ArrayList<>())  // ["Payment: PAY-123"]
    .build();

// При ошибке:
state.setStatus(SagaStatus.COMPENSATING);

// Откатываем в обратном порядке
if (state.getCompletedSteps().contains("Payment")) {
    commandPublisher.publishRefundPayment(...);
}
if (state.getCompletedSteps().contains("Inventory")) {
    commandPublisher.publishReleaseInventory(...);
}
```

## 📦 Event Sourcing элементы

### SagaState - Event Log

```java
@Data
public class SagaState {
    private String sagaId;
    private SagaStatus status;
    private List<String> completedSteps;  // Что выполнено
    private List<String> eventLog;        // История событий

    public void addEvent(String event) {
        eventLog.add(event);
        updatedAt = Instant.now();
    }
}
```
* Создание `SagaState`:
```java
        sagaState.status(SagaStatus.STARTED)
        sagaState.addEvent("OrderCreated: " + orderCreatedEvent.getOrderId());
        sagaStateRepository.save(sagaState);
        
        commandPublisher.publishProcessPayment(command);

        sagaState.setStatus(SagaStatus.PAYMENT_PROCESSING);
        sagaState.addEvent("ProcessPaymentCommand sent");
        sagaStateRepository.save(sagaState);
```
* Компенсация. Резервирование провалилось → возвращаем деньги:
```java

        state.setStatus(SagaStatus.COMPENSATING);
        state.addEvent("InventoryReservationFailed: " + inventoryReservationFailedEvent.getReason());
        stateRepository.save(state);

        commandPublisher.publishRefundPayment(publishRefundPayment);
        state.setStatus(SagaStatus.COMPENSATED);
        stateRepository.save(state);
```

**Преимущества:**
- Можно восстановить состояние саги после сбоя
- Audit trail (аудиторский след): видим всю историю транзакции. Audit trail — это хронологическая (по времени)   
и неизменяемая запись всех значимых действий, событий, изменений или транзакций в системе.
- Отладка: понятно, на каком этапе провал

## 🚀 Запуск проекта

### 1. Требования

- Java 21+
- Maven 3.9+
- Docker & Docker Compose

### 2. Сборка

```bash
# Сборка всех модулей
mvn clean package -DskipTests

# Или сборка в IntelliJ IDEA
```

### 3. Запуск Pulsar standalone (локально)

```bash
# Запустить только Pulsar
docker-compose up pulsar -d

# Проверить health
docker-compose ps
```

### 4. Запуск сервисов (локально из IDE)

Запустите в IntelliJ IDEA (или терминале):

1. `SagaOrchestratorApplication` (порт 8080)
2. `OrderServiceApplication` (порт 8081)
3. `PaymentServiceApplication` (порт 8082)
4. `InventoryServiceApplication` (порт 8083)

Все сервисы подключатся к Pulsar на `pulsar://localhost:6650`.

### 5. Тестирование

#### Успешный заказ

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

**Ожидаемый результат:**
```
✅ Logs в saga-orchestrator:
- OrderCreated received
- ProcessPaymentCommand sent
- PaymentProcessed received
- ReserveInventoryCommand sent
- InventoryReserved received
- Saga COMPLETED
```

#### Заказ с недостатком товара (компенсация)

```bash
# Заказать больше, чем есть на складе
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-002",
    "productCode": "PRODUCT-001",
    "quantity": 200,
    "totalAmount": 19999.99
  }'
```

**Ожидаемый результат:**
```
✅ Logs в saga-orchestrator:
- OrderCreated received
- ProcessPaymentCommand sent
- PaymentProcessed received
- ReserveInventoryCommand sent
- InventoryReservationFailed received
- Saga COMPENSATING
- RefundPaymentCommand sent
- Saga COMPENSATED
```

#### Проверка идемпотентности

```bash
# Отправить тот же заказ дважды (с тем же orderId) → второй игнорируется
```

### 6. Мониторинг Pulsar

Откройте Pulsar Admin UI:
```
http://localhost:8080/admin/v2/persistent/public/default
```

Или используйте `pulsar-admin` CLI:
```bash
docker exec -it pulsar bin/pulsar-admin topics list public/default
docker exec -it pulsar bin/pulsar-admin topics stats persistent://public/default/order-created
```

## 📚 Ключевые паттерны и концепции

### 1. Transactional Outbox (упрощенно)

**Проблема:** Как гарантировать, что изменение в БД и публикация события - атомарны?

**Решение (в production):**
```java
@Transactional
public void createOrder(Order order) {
    // 1. Сохранить заказ
    orderRepository.save(order);

    // 2. Сохранить событие в outbox таблицу (та же транзакция)
    outboxRepository.save(new OutboxEvent("OrderCreated", order));

    // 3. Отдельный процесс читает outbox и публикует в Pulsar
}
```

В этом проекте для простоты сразу публикуем в Pulsar.

### 2. Saga State Machine

```
STARTED → PAYMENT_PROCESSING → PAYMENT_COMPLETED
  → INVENTORY_RESERVING → COMPLETED

При ошибке:
  → COMPENSATING → COMPENSATED
```

### 3. Command vs Event

**Event** (факт, что-то произошло):
- `OrderCreated`, `PaymentProcessed`, `InventoryReserved`
- Публикуются участниками саги

**Command** (приказ выполнить действие):
- `ProcessPayment`, `ReserveInventory`, `RefundPayment`
- Отправляются оркестратором

### 4. Shared Subscription в Pulsar

```java
.subscriptionType(SubscriptionType.Shared)
```

**Преимущество:**
- Несколько инстансов сервиса могут обрабатывать сообщения параллельно
- Масштабирование: добавили инстанс → нагрузка распределяется
- Pulsar гарантирует: каждое сообщение обработано ровно одним consumer

## 🧪 Тестирование (опционально)

### Testcontainers + Pulsar

```java
@Testcontainers
class SagaIntegrationTest {

    @Container
    static PulsarContainer pulsar = new PulsarContainer("apachepulsar/pulsar:3.3.3");

    @Test
    void testSuccessfulSaga() {
        // Отправить OrderCreated
        // Проверить, что PaymentProcessed и InventoryReserved опубликованы
        // Проверить состояние саги: COMPLETED
    }

    @Test
    void testSagaCompensation() {
        // Симулировать недостаток товара
        // Проверить, что RefundPayment отправлен
        // Проверить состояние: COMPENSATED
    }
}
```

## 🎓 Учебные цели достигнуты

✅ **Гарантированная доставка:**
- At-least-once через Pulsar acknowledgements
- Exactly-once через idempotency keys
- Retry при ошибках

✅ **Откат транзакций:**
- Saga Orchestration для координации
- Compensating commands для отката
- Event sourcing для отслеживания состояния

✅ **Pulsar features (возможности):**
- Persistent topics для durability  
        Постоянные топики (persistent topics) — для durability (гарантированного сохранения сообщений на диске, даже после перезапуска брокера или сбоя)
- Shared subscriptions для масштабирования
- Deduplication для exactly-once

✅ **Понятность:**
- Комментарии в коде
- Четкое разделение ответственности
- Примеры успешного и провального сценариев

## 📖 Дополнительные улучшения (для production)

1. **Transactional Outbox Pattern**: атомарность БД + Pulsar
2. **Dead Letter Queue**: для сообщений, которые не удалось обработать
3. **Saga Timeout**: автоматическая компенсация при таймауте
4. **Distributed Tracing**: OpenTelemetry для отслеживания саг
5. **CQRS**: отдельные Read Models для запросов
6. **Pulsar Transactions**: атомарная отправка/получение нескольких сообщений
7. **Circuit Breaker**: при недоступности участника саги
8. **Saga Recovery**: восстановление после сбоя оркестратора

## 📚 Ресурсы

- [Saga Pattern - Microservices.io](https://microservices.io/patterns/data/saga.html)
- [Apache Pulsar Documentation](https://pulsar.apache.org/docs/next/)
- [Event Sourcing - Martin Fowler](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Idempotency - Stripe](https://stripe.com/docs/api/idempotent_requests)

## 🤝 Обратная связь

Это учебный проект. Для production требуются дополнительные механизмы надежности, мониторинга и отказоустойчивости.
