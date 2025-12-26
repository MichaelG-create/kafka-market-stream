from typing import Optional

from confluent_kafka import Consumer, KafkaException  # type: ignore

from market_streaming.application.ports import MessageConsumer


class ConfluentKafkaMessageConsumer(MessageConsumer):
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        try:
            self._consumer = Consumer(conf)
            self._consumer.subscribe([topic])
            print(
                f"✅ Consumer connected to Kafka at {bootstrap_servers}, "
                f"group '{group_id}', topic '{topic}'"
            )
        except KafkaException as e:
            print(f"❌ Failed to create consumer: {e}")
            raise

    def poll(self, timeout: float) -> Optional[bytes]:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            return None
        return msg.value()

    def close(self) -> None:
        self._consumer.close()
