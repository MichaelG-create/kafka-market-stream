from market_streaming.application.producer_services import MarketTickProducerService
from market_streaming.infrastructure.csv_tick_source import CsvTickSource
from market_streaming.infrastructure.kafka_tick_publisher import ConfluentKafkaTickPublisher


def build_producer_service() -> MarketTickProducerService:
    source = CsvTickSource(csv_path="data/indices_sample.csv")
    publisher = ConfluentKafkaTickPublisher(
        bootstrap_servers="localhost:9092",
        topic_name="market_indices_raw",
        client_id="market-indices-producer",
    )

    return MarketTickProducerService(
        source=source,
        publisher=publisher,
        delay_seconds=1.0,      # your US2-T4 delay
        progress_interval=10,
    )


def main() -> None:
    service = build_producer_service()
    service.run()


if __name__ == "__main__":
    main()
