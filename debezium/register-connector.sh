#!/bin/bash
echo "Waiting for Kafka Connect REST API..."
until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do
  echo "  not ready, retrying in 5s..."
  sleep 5
done

echo "Registering pagila connector..."
RESULT=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/debezium/connector.json)

if [ "$RESULT" = "201" ]; then
  echo "Connector registered (HTTP 201)"
elif [ "$RESULT" = "409" ]; then
  echo "Connector already exists (HTTP 409) — OK"
else
  echo "Unexpected HTTP $RESULT"
  exit 1
fi

curl -s http://localhost:8083/connectors/pagila-connector/status | python3 -m json.tool
