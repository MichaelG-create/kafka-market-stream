import json
import time
from datetime import datetime
from typing import Optional

from market_streaming.domain.models import MarketTick
from market_streaming.application.ports import (
    MessageConsumer,
    TickSink,
    RunMetrics,
    MetricsSink,
    LoggerPort,
)


class MarketTickConsumerService:
    """Consume raw messages, convert to MarketTick, insert via TickSink, record metrics and logs."""

    def __init__(
        self,
        consumer: MessageConsumer,
        sink: TickSink,
        metrics_sink: MetricsSink,
        logger: LoggerPort,
    ) -> None:
        self._consumer = consumer
        self._sink = sink
        self._metrics_sink = metrics_sink
        self._logger = logger

    def run(self, max_messages: Optional[int] = None) -> RunMetrics:
        messages_processed = 0
        errors = 0
        max_timestamp: Optional[str] = None

        start_time = time.time()
        run_started_at = datetime.utcnow().isoformat()

        # structured start log
        self._logger.info(
            "pipeline_start",
            {
                "run_started_at": run_started_at,
                "max_messages": max_messages,
            },
        )

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
                        # keep a lightweight console hint if you like
                        print(
                            f"✅ Inserted {messages_processed} messages "
                            f"(last: {tick.symbol} @ {tick.price})"
                        )

                    if max_messages is not None and messages_processed >= max_messages:
                        print("ℹ️  Reached max_messages limit, stopping consumer.")
                        break

                except Exception as exc:
                    errors += 1
                    # structured error log for individual message failures
                    self._logger.error(
                        "message_processing_error",
                        {
                            "error": str(exc),
                            "messages_processed": messages_processed,
                            "errors": errors,
                        },
                    )

        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            self._consumer.close()
            end_time = time.time()
            run_ended_at = datetime.utcnow().isoformat()
            elapsed_seconds = end_time - start_time

            metrics = RunMetrics(
                run_started_at=run_started_at,
                run_ended_at=run_ended_at,
                elapsed_seconds=elapsed_seconds,
                messages_processed=messages_processed,
                errors=errors,
                max_timestamp=max_timestamp,
            )

            # persist metrics (US4-T3)
            self._metrics_sink.insert_run_metrics(metrics)

            # structured end log (US4-T4)
            self._logger.info(
                "pipeline_end",
                {
                    "run_started_at": run_started_at,
                    "run_ended_at": run_ended_at,
                    "elapsed_seconds": elapsed_seconds,
                    "messages_processed": messages_processed,
                    "errors": errors,
                    "max_timestamp": max_timestamp,
                },
            )

        return metrics
