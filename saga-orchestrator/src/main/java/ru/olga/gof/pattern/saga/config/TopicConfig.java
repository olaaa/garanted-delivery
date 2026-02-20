package ru.olga.gof.pattern.saga.config;

/**
 * Конфигурация топиков Pulsar.
 *
 * NAMING CONVENTION:
 * - persistent:// - данные хранятся на диске (durability)
 * - tenant/namespace/topic - иерархия для изоляции
 *
 * ТИПЫ ТОПИКОВ:
 * - Events: публикуются сервисами после выполнения действий
 * - Commands: отправляются оркестратором для выполнения действий
 */
public class TopicConfig {

    // Tenant и namespace для изоляции
    public static final String TENANT = "public";
    public static final String NAMESPACE = "default";

    // События от Order Service
    public static final String ORDER_CREATED_TOPIC =
        String.format("persistent://%s/%s/order-created", TENANT, NAMESPACE);

    // Команды для Payment Service
    public static final String PROCESS_PAYMENT_TOPIC =
        String.format("persistent://%s/%s/process-payment", TENANT, NAMESPACE);
    public static final String REFUND_PAYMENT_TOPIC =
        String.format("persistent://%s/%s/refund-payment", TENANT, NAMESPACE);

    // События от Payment Service
    public static final String PAYMENT_PROCESSED_TOPIC =
        String.format("persistent://%s/%s/payment-processed", TENANT, NAMESPACE);
    public static final String PAYMENT_FAILED_TOPIC =
        String.format("persistent://%s/%s/payment-failed", TENANT, NAMESPACE);

    // Команды для Inventory Service
    public static final String RESERVE_INVENTORY_TOPIC =
        String.format("persistent://%s/%s/reserve-inventory", TENANT, NAMESPACE);
    public static final String RELEASE_INVENTORY_TOPIC =
        String.format("persistent://%s/%s/release-inventory", TENANT, NAMESPACE);

    // События от Inventory Service
    public static final String INVENTORY_RESERVED_TOPIC =
        String.format("persistent://%s/%s/inventory-reserved", TENANT, NAMESPACE);
    public static final String INVENTORY_RESERVATION_FAILED_TOPIC =
        String.format("persistent://%s/%s/inventory-reservation-failed", TENANT, NAMESPACE);

    // Subscription names для consumers
    public static final String SAGA_ORCHESTRATOR_SUBSCRIPTION = "saga-orchestrator-sub";
    public static final String PAYMENT_SERVICE_SUBSCRIPTION = "payment-service-sub";
    public static final String INVENTORY_SERVICE_SUBSCRIPTION = "inventory-service-sub";
}
