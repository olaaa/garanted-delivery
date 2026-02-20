package ru.olga.gof.pattern.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Saga Orchestrator - координатор распределенных транзакций.
 *
 * РОЛЬ В АРХИТЕКТУРЕ:
 * - Слушает события OrderCreated от Order Service
 * - Отправляет команды участникам (Payment, Inventory)
 * - Отслеживает состояние саги
 * - Управляет компенсациями при ошибках
 *
 * ГАРАНТИИ ДОСТАВКИ:
 * - Использует Pulsar acknowledgements для at-least-once
 * - Хранит состояние саги для идемпотентности
 * - Переотправляет команды при таймаутах
 */
@SpringBootApplication
public class SagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorApplication.class, args);
    }
}
