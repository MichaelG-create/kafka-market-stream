import csv
from typing import Iterable

from market_streaming.domain.models import MarketTick
from market_streaming.application.producer_services import TickSource


class CsvTickSource(TickSource):
    def __init__(self, csv_path: str, encoding: str = "utf-8") -> None:
        self._csv_path = csv_path
        self._encoding = encoding

    def read_ticks(self) -> Iterable[MarketTick]:
        print(f"📖 Reading data from {self._csv_path}...")
        try:
            with open(self._csv_path, "r", encoding=self._encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield MarketTick(
                        timestamp=row["timestamp"],
                        symbol=row["symbol"],
                        price=float(row["price"]),
                        volume=int(row["volume"]),
                    )
        except FileNotFoundError as exc:
            print(f"❌ CSV file not found: {self._csv_path}")
            raise
