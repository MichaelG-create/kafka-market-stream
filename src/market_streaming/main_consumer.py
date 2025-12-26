from market_streaming.application.consumer_services import (
    MarketTickConsumerService,
    log_run_metrics,
)
from market_streaming.infrastructure.kafka_consumer import ConfluentKafkaMessageConsumer
from market_streaming.infrastructure.duckdb_repository import DuckDBTickRepository


def build_consumer_service() -> MarketTickConsumerService:
    kafka_consumer = ConfluentKafkaMessageConsumer(
        bootstrap_servers="localhost:9092",
        topic="market_indices_raw",
        group_id="market-ticks-consumer",
    )
    tick_sink = DuckDBTickRepository(db_path="data/market_data.duckdb")

    return MarketTickConsumerService(
        consumer=kafka_consumer,
        sink=tick_sink,
    )


def main() -> None:
    service = build_consumer_service()
    metrics = service.run()
    log_run_metrics(metrics)


if __name__ == "__main__":
    main()
