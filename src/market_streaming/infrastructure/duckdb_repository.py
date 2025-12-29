import os

import duckdb  # type: ignore

from market_streaming.domain.models import MarketTick
from market_streaming.application.ports import TickSink


class DuckDBTickRepository(TickSink):
    def __init__(self) -> None:
        self.db_path = os.getenv("MARKET_DB_PATH", "data/market_data.duckdb")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._con = duckdb.connect(self.db_path)
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
        print(f"✅ DuckDB connected at {self.db_path}")

    def _get_connection(self):
        return duckdb.connect(self.db_path)

    def insert_tick(self, tick: MarketTick) -> None:
        with self._get_connection() as con:
            con.execute(
                """
                INSERT INTO market_ticks (ts, symbol, price, volume)
                VALUES (?, ?, ?, ?)
                """,
                [tick.timestamp, tick.symbol, tick.price, tick.volume],
            )

    # def close(self) -> None:
        # self._con.close()
