from market_streaming.application.consumer_services import (
    MarketTickConsumerService,
    log_run_metrics,
)
from market_streaming.infrastructure.kafka_consumer import ConfluentKafkaMessageConsumer
from market_streaming.infrastructure.duckdb_repository import DuckDBTickRepository
from market_streaming.infrastructure.duckdb_metrics_repository import DuckDBMetricsRepository


DB_PATH = "data/market_data.duckdb"


def build_consumer_service() -> tuple[MarketTickConsumerService, DuckDBMetricsRepository]:
    kafka_consumer = ConfluentKafkaMessageConsumer(
        bootstrap_servers="localhost:9092",
        topic="market_indices_raw",
        group_id="market-ticks-consumer",
    )
    tick_sink = DuckDBTickRepository(db_path=DB_PATH)
    metrics_sink = DuckDBMetricsRepository(db_path=DB_PATH)

    service = MarketTickConsumerService(
        consumer=kafka_consumer,
        sink=tick_sink,
    )
    return service, metrics_sink


def main() -> None:
    service, metrics_sink = build_consumer_service()

    try:
        metrics = service.run()
        log_run_metrics(metrics)
        metrics_sink.insert_run_metrics(metrics)
    finally:
        metrics_sink.close()


if __name__ == "__main__":
    main()
