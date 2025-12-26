import os

import duckdb  # type: ignore

from market_streaming.domain.models import MarketTick
from market_streaming.application.ports import TickSink


class DuckDBTickRepository(TickSink):
    def __init__(self, db_path: str = "data/market_data.duckdb") -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._con = duckdb.connect(db_path)
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS market_ticks (
                ts TIMESTAMP,
                symbol TEXT,
                price DOUBLE,
                volume BIGINT
            );
            """
        )
        print(f"✅ DuckDB connected at {db_path}")

    def insert_tick(self, tick: MarketTick) -> None:
        self._con.execute(
            """
            INSERT INTO market_ticks (ts, symbol, price, volume)
            VALUES (?, ?, ?, ?)
            """,
            [tick.timestamp, tick.symbol, tick.price, tick.volume],
        )

    def close(self) -> None:
        self._con.close()
