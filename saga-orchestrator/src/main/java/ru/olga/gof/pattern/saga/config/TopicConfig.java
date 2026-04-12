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
public final class TopicConfig {

    private TopicConfig() {}

    private static final String TENANT = "public";
    private static final String NAMESPACE = "default";

    private static String topic(String name) {
        return "persistent://%s/%s/%s".formatted(TENANT, NAMESPACE, name);
    }

    // События от Order Service
    public static final String ORDER_CREATED_TOPIC = topic("order-created");

    // Команды для Payment Service
    public static final String PROCESS_PAYMENT_TOPIC = topic("process-payment");
    public static final String REFUND_PAYMENT_TOPIC  = topic("refund-payment");

    // События от Payment Service
    public static final String PAYMENT_PROCESSED_TOPIC = topic("payment-processed");
    public static final String PAYMENT_FAILED_TOPIC    = topic("payment-failed");

    // Команды для Inventory Service
    public static final String RESERVE_INVENTORY_TOPIC = topic("reserve-inventory");
    public static final String RELEASE_INVENTORY_TOPIC = topic("release-inventory");

    // События от Inventory Service
    public static final String INVENTORY_RESERVED_TOPIC             = topic("inventory-reserved");
    public static final String INVENTORY_RESERVATION_FAILED_TOPIC   = topic("inventory-reservation-failed");

    // Subscription names для consumers
    public static final String SAGA_ORCHESTRATOR_SUBSCRIPTION = "saga-orchestrator-sub";
    public static final String PAYMENT_SERVICE_SUBSCRIPTION   = "payment-service-sub";
    public static final String INVENTORY_SERVICE_SUBSCRIPTION = "inventory-service-sub";
}