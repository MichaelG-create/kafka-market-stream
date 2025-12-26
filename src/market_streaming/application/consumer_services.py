import json
import time
from datetime import datetime
from typing import Optional

from market_streaming.domain.models import MarketTick
from market_streaming.application.ports import (
    MessageConsumer,
    TickSink,
    RunMetrics,
)


class MarketTickConsumerService:
    """Consume raw messages, convert to MarketTick, insert via TickSink, return run metrics."""

    def __init__(self, consumer: MessageConsumer, sink: TickSink) -> None:
        self._consumer = consumer
        self._sink = sink

    def run(self, max_messages: Optional[int] = None) -> RunMetrics:
        messages_processed = 0
        errors = 0
        max_timestamp: Optional[str] = None

        start_time = time.time()
        run_started_at = datetime.utcnow().isoformat()

        try:
            while True:
                raw = self._consumer.poll(timeout=1.0)

                if raw is None:
                    continue

                try:
                    payload = raw.decode("utf-8")
                    data = json.loads(payload)

                    tick = MarketTick(
                        timestamp=data["timestamp"],
                        symbol=data["symbol"],
                        price=float(data["price"]),
                        volume=int(data["volume"]),
                    )

                    self._sink.insert_tick(tick)
                    messages_processed += 1

                    ts = tick.timestamp
                    if ts is not None and (max_timestamp is None or ts > max_timestamp):
                        max_timestamp = ts

                    if messages_processed % 10 == 0:
                        print(
                            f"✅ Inserted {messages_processed} messages "
                            f"(last: {tick.symbol} @ {tick.price})"
                        )

                    if max_messages is not None and messages_processed >= max_messages:
                        print("ℹ️  Reached max_messages limit, stopping consumer.")
                        break

                except Exception as exc:
                    print(f"❌ Error processing message: {exc}")
                    errors += 1

        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            self._consumer.close()
            end_time = time.time()
            run_ended_at = datetime.utcnow().isoformat()
            elapsed_seconds = end_time - start_time

        return RunMetrics(
            run_started_at=run_started_at,
            run_ended_at=run_ended_at,
            elapsed_seconds=elapsed_seconds,
            messages_processed=messages_processed,
            errors=errors,
            max_timestamp=max_timestamp,
        )


def log_run_metrics(metrics: RunMetrics) -> None:
    print("\n📊 Run summary (US4):")
    print(f"   run_started_at:     {metrics.run_started_at}")
    print(f"   run_ended_at:       {metrics.run_ended_at}")
    print(f"   elapsed_seconds:    {metrics.elapsed_seconds:.2f}")
    print(f"   messages_processed: {metrics.messages_processed}")
    print(f"   errors:             {metrics.errors}")
    print(f"   max_timestamp:      {metrics.max_timestamp}")
