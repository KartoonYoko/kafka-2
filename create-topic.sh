#!/bin/sh
set -eu

BOOTSTRAP="${BOOTSTRAP_SERVER:-kafka1:29092}"
TOPIC="${TOPIC:-my-topic}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION_FACTOR:-2}"

echo "Ожидание Kafka (${BOOTSTRAP})..."
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "Kafka ещё не готова, повтор через 2 с..."
  sleep 2
done

echo "Создание топика ${TOPIC}: partitions=${PARTITIONS}, replication-factor=${REPLICATION}"
until kafka-topics --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --bootstrap-server "$BOOTSTRAP"
do
  echo "Не удалось создать топик (брокеры ещё поднимаются), повтор через 3 с..."
  sleep 3
done

echo "Топик ${TOPIC} готов:"
kafka-topics --describe --topic "$TOPIC" --bootstrap-server "$BOOTSTRAP"
