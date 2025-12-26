import json
from typing import Optional, Callable

from confluent_kafka import Producer, KafkaException  # type: ignore

from market_streaming.domain.models import MarketTick
from market_streaming.application.producer_services import TickPublisher


def default_delivery_report(err, msg) -> None:
    if err is not None:
        print(f"❌ Message delivery failed: {err}")


class ConfluentKafkaTickPublisher(TickPublisher):
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str,
        client_id: str = "market-indices-producer",
        delivery_callback: Optional[Callable[[Optional[Exception], str], None]] = None,
    ) -> None:
        self._topic_name = topic_name
        self._delivery_callback = delivery_callback or (lambda err, _topic: default_delivery_report(err, None))

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "retries": 3,
            "max.in.flight.requests.per.connection": 1,
        }

        try:
            self._producer = Producer(conf)
            print(f"✅ Producer connected to Kafka at {bootstrap_servers}")
        except KafkaException as e:
            print(f"❌ Failed to create producer: {e}")
            raise

    def _on_delivery(self, err, msg) -> None:
        # Delegate to higher-level callback (only pass err + topic name)
        if self._delivery_callback is not None:
            self._delivery_callback(err, msg.topic())

    def publish(self, tick: MarketTick) -> None:
        message_json = json.dumps(tick.to_dict())
        self._producer.produce(
            topic=self._topic_name,
            value=message_json.encode("utf-8"),
            callback=self._on_delivery,
        )
        # Process background delivery callbacks
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()
