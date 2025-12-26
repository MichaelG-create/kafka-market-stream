from dataclasses import dataclass
from typing import Iterable, Protocol, Dict, Any, Optional

from market_streaming.domain.models import MarketTick


class MarketDataSource(Protocol):
    def read_ticks(self) -> Iterable[MarketTick]:
        ...


class MessageSerializer(Protocol):
    def serialize(self, tick: MarketTick) -> bytes:
        ...


class MessagePublisher(Protocol):
    def publish(self, payload: bytes) -> None:
        ...

    def flush(self) -> None:
        ...


class MessageConsumer(Protocol):
    """Port for reading raw messages from Kafka (or any queue)."""

    def poll(self, timeout: float) -> Optional[bytes]:
        """Return raw message payload as bytes or None if no message."""
        ...

    def close(self) -> None:
        ...


class TickSink(Protocol):
    """Port for persisting MarketTick objects somewhere."""

    def insert_tick(self, tick: MarketTick) -> None:
        ...


@dataclass
class RunMetrics:
    run_started_at: str
    run_ended_at: str
    elapsed_seconds: float
    messages_processed: int
    errors: int
    max_timestamp: Optional[str]
