#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRODUCER_MAIN="market_streaming.main_producer"
CONSUMER_MAIN="market_streaming.main_consumer"
KAFKA_CONTAINER_NAME="kafka"
KAFKA_PORT=9092

check_kafka() {
  echo "[INFO] Checking Kafka on localhost:${KAFKA_PORT}..."
  if nc -z localhost "${KAFKA_PORT}" 2>/dev/null; then
    echo "[OK] Kafka broker reachable on localhost:${KAFKA_PORT}"
    return 0
  fi
  echo "[WARN] Kafka not reachable on localhost:${KAFKA_PORT}"
  return 1
}

start_kafka_if_needed() {
  if check_kafka; then
    return 0
  fi

  if docker ps -a --format '{{.Names}}' | grep -qx "${KAFKA_CONTAINER_NAME}"; then
    echo "[INFO] Kafka container '${KAFKA_CONTAINER_NAME}' exists, starting it..."
    docker start "${KAFKA_CONTAINER_NAME}" >/dev/null
  else
    echo "[INFO] Kafka container '${KAFKA_CONTAINER_NAME}' not found, creating it..."
    docker run -d \
      --name "${KAFKA_CONTAINER_NAME}" \
      -p 9092:9092 \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_LISTENERS=PLAINTEXT://localhost:29092,CONTROLLER://localhost:29093,PLAINTEXT_HOST://0.0.0.0:9092 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      apache/kafka:latest >/dev/null
  fi

  echo "[INFO] Waiting for Kafka to be reachable on localhost:${KAFKA_PORT}..."
  for i in {1..30}; do
    if nc -z localhost "${KAFKA_PORT}" 2>/dev/null; then
      echo "[OK] Kafka is up on localhost:${KAFKA_PORT}"
      return 0
    fi
    sleep 1
  done
  echo "[ERROR] Kafka did not become reachable on localhost:${KAFKA_PORT} in time."
  exit 1
}

run_producer_fg() {
  echo "[INFO] Starting producer in foreground..."
  cd "$PROJECT_ROOT"
  uv run python -m "$PRODUCER_MAIN"
  echo "[OK] Producer finished."
}

run_consumer_bg() {
  echo "[INFO] Starting consumer in background..."
  cd "$PROJECT_ROOT"
  # Consumer will stop by itself when idle_timeout_seconds triggers in Python
  uv run python -m "$CONSUMER_MAIN" > consumer.log 2>&1 &
  CONSUMER_PID=$!
  echo "[OK] Consumer started with PID ${CONSUMER_PID}"
}

main() {
  cd "$PROJECT_ROOT"
  start_kafka_if_needed

  # 1) Start consumer (background, with idle-based auto-stop)
  run_consumer_bg

  # 2) Give it a short head-start so it subscribes
  sleep 2

  # 3) Run producer in foreground: messages are consumed as they’re produced
  run_producer_fg

  # 4) Wait for consumer to finish naturally (idle timeout)
  echo "[INFO] Waiting for consumer to finish..."
  if wait "$CONSUMER_PID"; then
    echo "[OK] Consumer exited cleanly."
  else
    echo "[WARN] Consumer exited with non-zero status."
  fi

  echo "[SUCCESS] Pipeline run completed. Check DuckDB and logs."
}

main "$@"