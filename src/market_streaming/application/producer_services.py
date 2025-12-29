import time
from typing import Iterable, Protocol

from market_streaming.domain.models import MarketTick


class TickSource(Protocol):
    """Port for reading MarketTick objects (e.g., from CSV)."""

    def read_ticks(self) -> Iterable[MarketTick]: ...


class TickPublisher(Protocol):
    """Port for publishing MarketTick objects to some stream (Kafka)."""

    def publish(self, tick: MarketTick) -> None: ...

    def flush(self) -> None: ...


class MarketTickProducerService:
    """
    Application service: read MarketTick objects from a source,
    publish them via a publisher, with optional delay and logging.
    """

    def __init__(
        self,
        source: TickSource,
        publisher: TickPublisher,
        delay_seconds: float = 0.0,
        progress_interval: int = 10,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._delay_seconds = delay_seconds
        self._progress_interval = progress_interval

    def run(self) -> tuple[int, int]:
        messages_sent = 0
        errors = 0

        print("🚀 Starting Kafka Producer - Market Indices Stream\n")

        for tick in self._source.read_ticks():
            try:
                self._publisher.publish(tick)
                messages_sent += 1

                if (
                    self._progress_interval > 0
                    and messages_sent % self._progress_interval == 0
                ):
                    print(
                        f"✉️  Sent {messages_sent} messages "
                        f"(last: {tick.symbol} @ {tick.price})"
                    )

                if self._delay_seconds > 0:
                    time.sleep(self._delay_seconds)

            except Exception as exc:  # pylint: disable=broad-exception-caught
                print(f"❌ Error sending tick {tick}: {exc}")
                errors += 1

        print("\n⏳ Flushing remaining messages...")
        self._publisher.flush()

        print("\n📊 Summary:")
        print(f"   Messages sent: {messages_sent}")
        print(f"   Errors:        {errors}")

        return messages_sent, errors
