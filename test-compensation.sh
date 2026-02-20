#!/bin/bash

# Скрипт для тестирования компенсации (недостаток товара на складе)

echo "🔄 Creating order that will trigger compensation (insufficient inventory)..."

curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-002",
    "productCode": "PRODUCT-001",
    "quantity": 200,
    "totalAmount": 19999.99
  }' | jq '.'

echo ""
echo "✅ Check saga-orchestrator logs for:"
echo "   - OrderCreated received"
echo "   - PaymentProcessed received"
echo "   - InventoryReservationFailed received"
echo "   - Saga COMPENSATING"
echo "   - RefundPaymentCommand sent"
echo "   - Saga COMPENSATED"
