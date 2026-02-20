# Топология системы

## Компоненты и их взаимодействие

```
┌─────────┐
│ Client  │
└────┬────┘
     │ HTTP POST /api/orders
     ▼
┌──────────────────┐
│  Order Service   │ (8081)
│                  │
│ - Create order   │
│ - Publish event  │
└────────┬─────────┘
         │ OrderCreated
         │
         ▼
    ┌────────────────────────┐
    │   Apache Pulsar        │
    │                        │
    │ Topics:                │
    │ - order-created        │
    │ - process-payment      │
    │ - payment-processed    │
    │ - payment-failed       │
    │ - reserve-inventory    │
    │ - inventory-reserved   │
    │ - inventory-failed     │
    │ - refund-payment       │
    │ - release-inventory    │
    └───┬─────────┬──────────┘
        │         │
        │         │
   ┌────▼─────────▼────┐
   │ Saga Orchestrator │ (8080)
   │                   │
   │ - Coordinate saga │
   │ - Send commands   │
   │ - Handle events   │
   │ - Compensate      │
   └─┬───────────────┬─┘
     │               │
     │ Commands      │ Events
     │               │
┌────▼─────┐   ┌────▼──────┐
│ Payment  │   │ Inventory │
│ Service  │   │  Service  │
│ (8082)   │   │  (8083)   │
│          │   │           │
│ - Process│   │ - Reserve │
│ - Refund │   │ - Release │
└──────────┘   └───────────┘
```

## Потоки данных

### 1. Успешный заказ (Happy Path)

```
Client
  ↓ POST /orders
Order Service
  ↓ OrderCreated → Pulsar
Saga Orchestrator
  ↓ ProcessPayment → Pulsar
Payment Service
  ↓ PaymentProcessed → Pulsar
Saga Orchestrator
  ↓ ReserveInventory → Pulsar
Inventory Service
  ↓ InventoryReserved → Pulsar
Saga Orchestrator
  ✅ Saga COMPLETED
```

### 2. Заказ с компенсацией

```
Client
  ↓ POST /orders
Order Service
  ↓ OrderCreated → Pulsar
Saga Orchestrator
  ↓ ProcessPayment → Pulsar
Payment Service
  ↓ PaymentProcessed → Pulsar ✅
Saga Orchestrator
  ↓ ReserveInventory → Pulsar
Inventory Service
  ↓ InventoryReservationFailed → Pulsar ❌
Saga Orchestrator
  ↓ 🔄 Start Compensation
  ↓ RefundPayment → Pulsar
Payment Service
  ↓ Process Refund ✅
Saga Orchestrator
  ✅ Saga COMPENSATED
```

## Pulsar Topics структура

```
persistent://public/default/
├── order-created              (Event)
├── process-payment            (Command)
├── payment-processed          (Event)
├── payment-failed             (Event)
├── refund-payment             (Command - Compensation)
├── reserve-inventory          (Command)
├── inventory-reserved         (Event)
├── inventory-reservation-failed (Event)
└── release-inventory          (Command - Compensation)
```

## Гарантии доставки на каждом уровне

```
┌────────────────────────────────────────────────────┐
│ Producer (Order Service)                           │
│ - Synchronous send (wait for ack)                 │
│ - Retry on failure                                 │
│ - Message key = idempotencyKey                     │
└───────────────────┬────────────────────────────────┘
                    │
                    ▼
┌────────────────────────────────────────────────────┐
│ Pulsar Broker                                      │
│ - Persistent storage (BookKeeper)                 │
│ - Deduplication enabled                            │
│ - Replication (multi-broker setup in production)  │
└───────────────────┬────────────────────────────────┘
                    │
                    ▼
┌────────────────────────────────────────────────────┐
│ Consumer (Saga Orchestrator)                       │
│ - Shared subscription (horizontal scaling)         │
│ - Idempotency check (state repository)            │
│ - Process message                                  │
│ - Acknowledge AFTER success                        │
│ - Negative acknowledge on failure → retry         │
└────────────────────────────────────────────────────┘
```

## Масштабирование

### Горизонтальное масштабирование

```
┌──────────────┐
│ Order Service│ Instance 1
└──────────────┘
┌──────────────┐
│ Order Service│ Instance 2
└──────────────┘
       ↓
   Load Balancer
       ↓
    Pulsar (single topic: order-created)
       ↓
┌─────────────────────┐
│ Saga Orchestrator 1 │ ← Shared Subscription
├─────────────────────┤
│ Saga Orchestrator 2 │ ← Получают разные сообщения
└─────────────────────┘
```

**Shared Subscription:**
- Pulsar распределяет сообщения между consumers
- Каждое сообщение обрабатывается только одним consumer
- Масштабирование: добавили instance → throughput вырос

### Failover

```
┌─────────────────────┐
│ Saga Orchestrator 1 │ ← Active
├─────────────────────┤
│ Saga Orchestrator 2 │ ← Standby
└─────────────────────┘

Instance 1 упал:
↓
Pulsar автоматически переключает на Instance 2
↓
Unacked messages переотправляются Instance 2
```

## State Management

### SagaState Repository

```
┌─────────────────────────────┐
│ SagaState                   │
│                             │
│ sagaId: SAGA-123            │
│ orderId: ORD-456            │
│ status: PAYMENT_COMPLETED   │
│                             │
│ completedSteps:             │
│   - Payment: PAY-789        │
│                             │
│ eventLog:                   │
│   - OrderCreated            │
│   - ProcessPaymentCommand   │
│   - PaymentProcessed        │
│                             │
│ createdAt: 2024-01-15 10:00 │
│ updatedAt: 2024-01-15 10:01 │
└─────────────────────────────┘
```

**Используется для:**
- Идемпотентность (уже обработали событие?)
- Компенсация (какие шаги откатить?)
- Мониторинг (где зависла сага?)
- Recovery (восстановить после сбоя)

## Мониторинг точки

### Метрики для сбора

**Saga Orchestrator:**
- Саг в процессе (by status)
- Саг завершенных (success / compensated / failed)
- Latency саги (от OrderCreated до COMPLETED)
- Компенсаций в час

**Payment/Inventory Services:**
- Команд обработано
- Команд провалено
- Дублей обнаружено (idempotency)
- Latency обработки

**Pulsar:**
- Backlog size (непрочитанные сообщения)
- Throughput (msg/sec)
- Latency (publish to consume)
- Consumer lag

### Alerting условия

⚠️ **Критично:**
- Backlog > 10000 сообщений
- Saga latency > 30 секунд
- Compensation rate > 10%

⚠️ **Внимание:**
- Consumer lag > 1 минута
- Idempotency duplicates > 5% (возможно, проблема с retries)

---

Эта топология демонстрирует, как компоненты взаимодействуют для обеспечения гарантированной доставки и отката транзакций.
