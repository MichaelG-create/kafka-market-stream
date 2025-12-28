from market_streaming.application.consumer_services import (
    MarketTickConsumerService,
)
from market_streaming.infrastructure.kafka_consumer import ConfluentKafkaMessageConsumer
from market_streaming.infrastructure.duckdb_repository import DuckDBTickRepository
from market_streaming.infrastructure.duckdb_metrics_repository import DuckDBMetricsRepository
from market_streaming.infrastructure.json_file_logger import JsonFileLogger

from market_streaming.cli_helpers import print_run_metrics


DB_PATH = "data/market_data.duckdb"


def build_consumer_service() -> MarketTickConsumerService:
    kafka_consumer = ConfluentKafkaMessageConsumer(
        bootstrap_servers="localhost:9092",
        topic="market_indices_raw",
        group_id="market-ticks-consumer",
    )
    tick_sink = DuckDBTickRepository(db_path=DB_PATH)
    metrics_sink = DuckDBMetricsRepository(db_path=DB_PATH)
    logger = JsonFileLogger()

    service = MarketTickConsumerService(
        consumer=kafka_consumer,
        sink=tick_sink,
        metrics_sink=metrics_sink,
        logger=logger,
    )
    return service


def main() -> None:
    service = build_consumer_service()
    metrics = service.run()
    print_run_metrics(metrics)


if __name__ == "__main__":
    main()
