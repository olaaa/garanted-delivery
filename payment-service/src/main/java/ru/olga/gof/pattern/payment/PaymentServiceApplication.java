package ru.olga.gof.pattern.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment Service - сервис обработки платежей.
 *
 * РОЛЬ В SAGA:
 * - Слушает команды ProcessPayment и RefundPayment
 * - Обрабатывает платеж (с внешним payment gateway или mock)
 * - Публикует события PaymentProcessed или PaymentFailed
 *
 * ИДЕМПОТЕНТНОСТЬ:
 * - Проверяет idempotencyKey перед обработкой
 * - Повторная команда с тем же ключом → возвращает тот же результат
 *
 * COMPENSATING ACTION:
 * - RefundPayment откатывает успешный платеж
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
