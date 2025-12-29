import os
import duckdb  # type: ignore

from market_streaming.application.ports import RunMetrics, MetricsSink


class DuckDBMetricsRepository(MetricsSink):
    def __init__(self) -> None:
        self.db_path = os.getenv("MARKET_DB_PATH", "data/market_data.duckdb")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._con = duckdb.connect(self.db_path)
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
        print(f"✅ DuckDB metrics repository ready at {self.db_path}")

    def _get_connection(self):
        return duckdb.connect(self.db_path)

    def insert_run_metrics(self, metrics: RunMetrics) -> None:
        with self._get_connection() as con:
            con.execute(
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

    # def close(self) -> None:
    #     self._con.close()
