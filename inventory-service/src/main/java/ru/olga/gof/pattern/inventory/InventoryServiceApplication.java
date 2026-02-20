package ru.olga.gof.pattern.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Inventory Service - сервис управления складскими запасами.
 *
 * РОЛЬ В SAGA:
 * - Слушает команды ReserveInventory и ReleaseInventory
 * - Резервирует товар для заказа
 * - Публикует события InventoryReserved или InventoryReservationFailed
 *
 * КОМПЕНСАЦИЯ:
 * - ReleaseInventory освобождает резервирование
 * - Вызывается при откате саги (если платеж провалился и т.д.)
 */
@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
