"""
US3-T1: Create DuckDB database and market_ticks table.
"""

import os
import duckdb  # pip install duckdb

DB_PATH = os.path.join("data", "market_data.duckdb")


def main() -> None:
    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(DB_PATH)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS market_ticks (
            ts TIMESTAMP,
            symbol TEXT,
            price DOUBLE,
            volume BIGINT
        );
        """
    )

    # Simple sanity check: list tables
    tables = con.execute("SHOW TABLES;").fetchall()
    print("✅ DuckDB connected at:", DB_PATH)
    print("📋 Tables:", tables)

    con.close()


if __name__ == "__main__":
    main()
