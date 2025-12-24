"""
US3-T2: Kafka consumer that reads from topic `market_indices_raw`
and inserts rows into DuckDB table `market_ticks`.
"""

import json
import os
import sys
import time
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaException  # pip install confluent-kafka
import duckdb  # pip install duckdb


KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "market_indices_raw"
GROUP_ID = "market-ticks-consumer"

DB_PATH = os.path.join("data", "market_data.duckdb")


def create_duckdb_connection():
    if not os.path.exists(DB_PATH):
        print(f"❌ DuckDB database not found at {DB_PATH}. Run create_market_ticks_table.py first.")
        sys.exit(1)

    con = duckdb.connect(DB_PATH)
    # Optional sanity check
    con.execute("CREATE TABLE IF NOT EXISTS market_ticks (ts TIMESTAMP, symbol TEXT, price DOUBLE, volume BIGINT);")
    return con


def create_consumer() -> Consumer:
    conf: Dict[str, Any] = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # start from beginning if no committed offset
        "enable.auto.commit": True,       # fine for this prototype
    }
    try:
        consumer = Consumer(conf)
        print(f"✅ Consumer connected to Kafka at {KAFKA_BROKER}, group '{GROUP_ID}'")
        return consumer
    except KafkaException as e:
        print(f"❌ Failed to create consumer: {e}")
        raise


def insert_tick(con, tick: Dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO market_ticks (ts, symbol, price, volume)
        VALUES (?, ?, ?, ?)
        """,
        [
            tick["timestamp"],
            tick["symbol"],
            float(tick["price"]),
            int(tick["volume"]),
        ],
    )


def run_consumer(max_messages: int | None = None) -> None:
    con = create_duckdb_connection()
    consumer = create_consumer()
    consumer.subscribe([TOPIC_NAME])

    print(f"📥 Subscribed to topic '{TOPIC_NAME}'")
    messages_processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout

            if msg is None:
                # no message within timeout, continue polling
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            try:
                payload = msg.value().decode("utf-8")
                data = json.loads(payload)

                insert_tick(con, data)
                messages_processed += 1

                if messages_processed % 10 == 0:
                    print(f"✅ Inserted {messages_processed} messages (last: {data['symbol']} @ {data['price']})")

                if max_messages is not None and messages_processed >= max_messages:
                    print("ℹ️  Reached max_messages limit, stopping consumer.")
                    break

            except Exception as exc:
                print(f"❌ Error processing message at offset {msg.offset()}: {exc}")

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    finally:
        print("🧹 Closing consumer and DuckDB connection...")
        consumer.close()
        con.close()
        print(f"📊 Total messages inserted this run: {messages_processed}")


if __name__ == "__main__":
    # For US3-T2, it's enough to insert some messages and verify via SELECT.
    run_consumer()