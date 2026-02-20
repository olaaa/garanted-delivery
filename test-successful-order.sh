#!/bin/bash

# Скрипт для тестирования успешного создания заказа

echo "🚀 Creating successful order..."

curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productCode": "PRODUCT-001",
    "quantity": 2,
    "totalAmount": 199.99
  }' | jq '.'

echo ""
echo "✅ Check saga-orchestrator logs for:"
echo "   - OrderCreated received"
echo "   - PaymentProcessed received"
echo "   - InventoryReserved received"
echo "   - Saga COMPLETED"
