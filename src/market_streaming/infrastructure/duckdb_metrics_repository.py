import os
import duckdb  # type: ignore

from market_streaming.application.ports import RunMetrics, MetricsSink


class DuckDBMetricsRepository(MetricsSink):
    def __init__(self, db_path: str = "data/market_data.duckdb") -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._db_path = db_path
        self._con = duckdb.connect(db_path)
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                run_started_at     TIMESTAMP,
                run_ended_at       TIMESTAMP,
                elapsed_seconds    DOUBLE,
                messages_processed BIGINT,
                errors             BIGINT,
                max_timestamp      TIMESTAMP
            );
            """
        )
        print(f"✅ DuckDB metrics repository ready at {db_path}")

    def insert_run_metrics(self, metrics: RunMetrics) -> None:
        self._con.execute(
            """
            INSERT INTO pipeline_metrics (
                run_started_at,
                run_ended_at,
                elapsed_seconds,
                messages_processed,
                errors,
                max_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                metrics.run_started_at,
                metrics.run_ended_at,
                float(metrics.elapsed_seconds),
                int(metrics.messages_processed),
                int(metrics.errors),
                metrics.max_timestamp,
            ],
        )

    def close(self) -> None:
        self._con.close()
