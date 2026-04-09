# Архитектурные решения и обоснования

## Выбор Saga Orchestration vs Choreography

### Saga Orchestration (реализовано в проекте)

**Как работает:**
- Центральный **Saga Orchestrator** координирует выполнение
- Оркестратор отправляет **команды** участникам (ProcessPayment, ReserveInventory)
- Участники отвечают **событиями** (PaymentProcessed, InventoryReserved)
- Оркестратор хранит **состояние саги** и управляет компенсациями

**Преимущества:**  
✅ **Централизованная логика** - весь flow в одном месте (легко понять)  
Flow -- Последовательность шагов обработки запроса / события / команды  
    Просто: поток, поток выполнения, сценарий  
✅ **Явное управление** - оркестратор четко знает порядок шагов  
✅ **Простота отладки** - можно посмотреть состояние саги в одном месте  
✅ **Легкость компенсаций** - оркестратор знает, какие шаги откатить и в каком порядке  
✅ **Добавление новых шагов** - изменения только в оркестраторе  
✅ **Мониторинг** - легко видеть прогресс всех саг  
✅ **Таймауты** - оркестратор может автоматически откатить при таймауте  

**Недостатки:**  
❌ **Single point of failure** - если оркестратор упал, саги блокируются. Нужна High Availability -- высокая доступность  
❌ **Coupling** - оркестратор знает о всех участниках  
❌ **Масштабирование** - оркестратор может стать узким местом  
❌ **Зависимость** - изменение участника может потребовать изменения оркестратора  

### Saga Choreography (альтернатива)

**Как работает:**
- Нет центрального координатора
- Каждый сервис слушает события и реагирует на них
- Например: Order Service публикует OrderCreated → Payment Service слушает и обрабатывает → публикует PaymentProcessed → Inventory Service слушает...

**Преимущества:**  
✅ **Decoupling** - сервисы не знают друг о друге  
✅ **Нет single point of failure**  
✅ **Масштабирование** - каждый сервис масштабируется независимо  
✅ **Гибкость** - легко добавить новых участников (просто слушают события)  

**Недостатки:**  
❌ **Сложность понимания** - flow размазан по разным сервисам  
❌ **Отладка** - сложно отследить, где застряла сага  
❌ **Компенсации** - каждый сервис должен сам понять, нужна ли компенсация  
❌ **Циклические зависимости** - событие A → B → C → A (сложно избежать)  
❌ **Мониторинг** - сложно увидеть состояние саги  

### Вывод для учебного проекта

**Orchestration выбран для демонстрации**, потому что:
1. Легче понять поток выполнения (важно для обучения)
2. Проще показать компенсации
3. Код более читаемый и структурированный

**Для production** выбор зависит от требований:
- Если важна понятность и контроль → Orchestration
- Если важна decoupling и масштабируемость → Choreography

---

## Гарантии доставки в Apache Pulsar

### 1. At-Least-Once Delivery

**Механизм:**
```
Producer → Broker → Consumer
                ↓
            Acknowledge
```

- Producer отправляет сообщение и ждет подтверждения от Broker
- Broker хранит сообщение на диске (persistent topic)
- Consumer получает сообщение
- Consumer обрабатывает
- Consumer отправляет **acknowledge** → Broker удаляет из очереди
- Если acknowledge не пришел (таймаут) → Broker переотправляет

**В коде:**
```java
// Producer: ждем подтверждения
MessageId id = producer.send(message);  // Блокируется до ack от брокера

// Consumer: acknowledge ПОСЛЕ обработки
Message msg = consumer.receive();
processMessage(msg);
consumer.acknowledge(msg);  // ← ВАЖНО: после, не до!
```

**Гарантия:** Сообщение будет доставлено хотя бы один раз (может быть дубль при сбое).

### 2. Exactly-Once Semantics

**Проблема:** At-least-once может привести к дублям:
```
Consumer получил → обработал → отправил ack → сеть упала
→ Broker не получил ack → переотправил
→ Consumer обработал повторно (дубль!)
```

**Решение 1: Idempotency Keys**

Каждое сообщение содержит уникальный ключ:
```java
// Producer
message.key(idempotencyKey);

// Consumer
if (alreadyProcessed(message.getKey())) {
    consumer.acknowledge(message);  // Игнорируем дубль
    return;
}
processMessage(message);
markAsProcessed(message.getKey());
consumer.acknowledge(message);
```

**Решение 2: Pulsar Deduplication**

Включить на брокере:
```properties
brokerDeduplicationEnabled=true
```

Pulsar автоматически отбрасывает дубли на основе `producerName + sequenceId`.

**Решение 3: Pulsar Transactions**

```java
Transaction txn = pulsarClient.newTransaction().build().get();

// Атомарная операция: получить + отправить
Message msg = consumer.receive();
producer.newMessage(txn).value(data).send();
consumer.acknowledgeAsync(msg.getMessageId(), txn);

txn.commit();  // Все или ничего
```

### 3. Ordering Guarantees

**В рамках partition:**
- Pulsar гарантирует порядок сообщений
- Если используется message key → сообщения с одним ключом идут в один partition

```java
producer.newMessage()
    .key("order-123")  // Все сообщения для заказа 123 → в один partition
    .value(data)
    .send();
```

**Shared subscription:**
- Сообщения распределяются между consumers
- Порядок внутри одного ключа сохраняется

---

## Event Sourcing элементы

### Что такое Event Sourcing?

**Традиционный подход:**
```
Order: { id: 1, status: "COMPLETED", total: 100 }
```
Храним текущее состояние.

**Event Sourcing:**
```
Events:
1. OrderCreated { id: 1, total: 100 }
2. PaymentProcessed { orderId: 1, amount: 100 }
3. InventoryReserved { orderId: 1, productCode: "A" }
4. OrderCompleted { orderId: 1 }
```
Храним поток событий. Состояние = применение всех событий.

### Как используется в проекте

**SagaState хранит event log:**
```java
@Data
public class SagaState {
    private List<String> eventLog;  // История событий

    public void addEvent(String event) {
        eventLog.add(event);
    }
}
```

**Восстановление состояния:**
```java
SagaState state = loadFromEventLog(sagaId);
// Проигрываем события:
// - "OrderCreated" → status = STARTED
// - "PaymentProcessed" → status = PAYMENT_COMPLETED
// - "InventoryReserved" → status = COMPLETED
```

**Преимущества:**
✅ **Audit trail** - полная история транзакции
✅ **Recovery** - можно восстановить состояние после сбоя
✅ **Time travel** - можно посмотреть состояние в прошлом
✅ **Debugging** - видно, где именно провал

---

## Idempotency Pattern

### Зачем нужна идемпотентность?

**Проблема:**
```
Client → OrderService: Create Order
Client не получил ответ (таймаут)
Client → OrderService: Create Order (повтор)
→ Два заказа!
```

**Решение:**
```java
@PostMapping("/orders")
public Response createOrder(@RequestBody CreateOrderRequest request) {
    String idempotencyKey = request.getIdempotencyKey();

    // Проверяем, не обрабатывали ли уже
    if (orderExists(idempotencyKey)) {
        return getExistingOrder(idempotencyKey);
    }

    Order order = createNewOrder(request);
    return order;
}
```

### В проекте

**В событиях:**
```java
OrderCreatedEvent event = OrderCreatedEvent.builder()
    .orderId(orderId)
    .idempotencyKey(orderId)  // Обычно = orderId
    .build();
```

**В обработчиках:**
```java
if (processedPayments.containsKey(command.getIdempotencyKey())) {
    log.warn("Already processed");
    return;
}
processPayment(command);
processedPayments.put(command.getIdempotencyKey(), result);
```

**В production:**
- Хранить в БД (не в памяти)
- TTL: удалять старые ключи через N дней
- Composite key: `customerId + timestamp` для natural deduplication

---

## Compensating Transactions

### Концепция

Распределенная транзакция не может использовать 2PC (Two-Phase Commit), потому что:
- Блокирует ресурсы надолго
- Не работает при недоступности участника
- Не масштабируется

**Saga Pattern:** каждый шаг выполняется независимо, при ошибке - компенсация.

### Forward Recovery vs Backward Recovery

**Backward Recovery (реализовано):**
```
Order → Payment → Inventory [FAIL]
        ↓
      Refund
```
Откатываем успешные шаги.

**Forward Recovery:**
```
Order → Payment [FAIL] → Retry Payment → Success
```
Пытаемся завершить сагу (retry).

### Semantic Lock Pattern

**Проблема:** Пока сага выполняется, заказ в промежуточном состоянии.

**Решение:**
```java
Order {
    status: "PENDING",      // Semantic lock
    sagaId: "SAGA-123"      // Связь с сагой
}
```

Другие операции видят `PENDING` и знают, что нельзя трогать.

### Compensating Actions Design

**Правила:**
1. Компенсация должна быть **идемпотентной** (можно вызвать несколько раз)
2. Компенсация должна быть **ретраябельной** (можно повторить при ошибке)
3. Не все действия компенсируемы (отправка email - нет; платеж - да)

**В коде:**
```java
// Действие
ProcessPaymentCommand → PaymentProcessed

// Компенсация (обратное действие)
RefundPaymentCommand → PaymentRefunded
```

---

## Transactional Outbox Pattern

### Проблема

```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);        // 1. Сохранили в БД
    pulsar.publish(OrderCreated);       // 2. Опубликовали событие
}
```

**Что может пойти не так:**
- БД commit успешен → Pulsar publish провалился → событие потеряно
- Pulsar publish успешен → БД commit откатился → событие опубликовано без заказа  
  
### Как работает @Transactional в Spring 
* Метод помечен @Transactional → Spring создаёт прокси вокруг твоего бина.  
* Начало транзакции — перед вызовом метода.  
* Commit или rollback происходит не сразу после repository.save(), а после успешного завершения  
    всего метода (или при выбросе исключения).
Фактический commit в БД — самый последний шаг, уже после выхода из твоего кода.  

**Нет атомарности** между БД и Pulsar!

### Решение

**Outbox Table:**
```sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    event_type VARCHAR,
    payload JSON,
    created_at TIMESTAMP,
    published BOOLEAN DEFAULT FALSE
);
```

**В транзакции:**
```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);
    outboxRepository.save(new OutboxEvent("OrderCreated", order));
    // commit: либо оба, либо ни одного
}
```

**Отдельный процесс (Outbox Relay -- отдельный ретранслятор/пересылатель):**
```java
while (true) {
    List<OutboxEvent> pending = outboxRepository.findUnpublished();
    for (OutboxEvent event : pending) {
        pulsar.publish(event.getPayload());
        outboxRepository.markPublished(event.getId());
    }
}
```

**Гарантия:** Если заказ в БД → событие будет опубликовано (at-least-once).

### В проекте

Для простоты проект **не использует** outbox pattern (публикация сразу в Pulsar).

Для production:
- Добавить outbox table
- Использовать CDC (Change Data Capture) или polling для relay
- Библиотека: Debezium + Pulsar Connector

---

## Pulsar vs Kafka

### Почему Pulsar для этого проекта?

| Feature | Pulsar | Kafka |
|---------|--------|-------|
| **Deduplication** | Встроенная (producerName + seqId) | Нужно реализовывать |
| **Transactions** | ✅ Native support | ✅ Есть, но сложнее |
| **Multi-tenancy** | ✅ Tenant/Namespace/Topic | ❌ Нет |
| **Geo-replication** | ✅ Встроенная | Требует MirrorMaker |
| **Ordering** | Per-partition + key | Per-partition + key |
| **Scalability** | Compute/Storage разделены | Coupled |
| **Latency** | Ниже (благодаря BookKeeper) | Выше |

**Pulsar преимущества для Saga:**
- Встроенная дедупликация → проще exactly-once
- Transactions → атомарная publish/ack нескольких сообщений
- Geo-replication → для multi-region saga

**Kafka преимущества:**
- Более зрелая экосистема (Kafka Streams, ksqlDB)
- Больше инструментов мониторинга
- Более широкое adoption

### Вывод

Для учебного проекта по гарантированной доставке **Pulsar лучше подходит**, потому что из коробки дает больше features для exactly-once и транзакций.

---

## Дальнейшие улучшения

### 1. Distributed Tracing

Добавить OpenTelemetry для отслеживания саг:
```
Trace ID: abc-123
├── Order Service: create order (100ms)
├── Saga Orchestrator: start saga (10ms)
├── Payment Service: process payment (500ms)
└── Inventory Service: reserve inventory (200ms)
Total: 810ms
```

### 2. Circuit Breaker

Если Payment Service недоступен:
```java
@CircuitBreaker(name = "payment", fallbackMethod = "paymentFallback")
public void processPayment(Command cmd) { ... }
```

### 3. Saga Timeout

Автоматический откат, если сага зависла:
```java
if (saga.getCreatedAt().plus(30, MINUTES).isBefore(now())) {
    startCompensation(saga);
}
```

### 4. CQRS (Command Query Responsibility Segregation)

Разделить write (saga) и read (запросы):
```
Write Side: Saga Orchestrator → Events → Write DB
Read Side: Events → Projection → Read DB (denormalized)
```

### 5. Dead Letter Queue

Для сообщений, которые не удалось обработать после N попыток:
```java
.deadLetterPolicy(DeadLetterPolicy.builder()
    .maxRedeliverCount(5)
    .deadLetterTopic("dlq-payment")
    .build())
```

---

Этот документ дополняет README.md архитектурными деталями и обоснованиями решений.
