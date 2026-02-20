package ru.olga.gof.pattern.order.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.olga.gof.pattern.order.dto.CreateOrderRequest;
import ru.olga.gof.pattern.order.dto.CreateOrderResponse;
import ru.olga.gof.pattern.order.service.OrderService;

/**
 * REST API для создания заказов.
 *
 * ENTRY POINT распределенной транзакции:
 * POST /api/orders → OrderCreated event → Saga начинается
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Создать новый заказ.
     *
     * Синхронный ответ клиенту: заказ создан и событие опубликовано.
     * Дальнейшая обработка (платеж, резервирование) - асинхронная.
     */
    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
            @RequestBody CreateOrderRequest request) {

        log.info("Received create order request: customerId={}, productCode={}, quantity={}",
                 request.getCustomerId(), request.getProductCode(), request.getQuantity());

        CreateOrderResponse response = orderService.createOrder(request);

        log.info("Order created: orderId={}, sagaId={}",
                 response.getOrderId(), response.getSagaId());

        return ResponseEntity.ok(response);
    }

    /**
     * Получить статус заказа (можно добавить).
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<String> getOrderStatus(@PathVariable String orderId) {
        // В production: запрос к БД или Read Model (CQRS)
        return ResponseEntity.ok("Order status for " + orderId + ": PROCESSING");
    }
}
