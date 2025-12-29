#!/usr/bin/env python3
# duckdb-setup.py - TASK 2: DuckDB Schema Setup
# Run: python3 duckdb-setup.py

import duckdb

from pathlib import Path


def setup_duckdb_schema():
    """TASK 2: DuckDB Schema (1min)"""
    print("🚀 TASK 2: DuckDB Schema (1min)")

    # Ensure data directory exists
    db_file = Path("data/market_data.duckdb")
    db_file.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(str(db_file))

    # Run schema setup
    con.execute(
        """
        -- Add timestamp column (safe - won't overwrite existing)
        ALTER TABLE market_ticks 
        ADD COLUMN IF NOT EXISTS timestamp_received TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    """
    )

    con.execute(
        """
        -- Metrics view
        CREATE OR REPLACE VIEW market_metrics AS
        SELECT 
          symbol,
          COUNT(*) as tick_count,
          ROUND(AVG(price), 2) as avg_price,
          MAX(price) as max_price,
          MIN(price) as min_price,
          DATE_DIFF('SECOND', MIN(timestamp_received), MAX(timestamp_received)) as session_duration_s
        FROM market_ticks 
        GROUP BY symbol;
    """
    )

    con.execute(
        """
        -- Live ticks
        CREATE OR REPLACE VIEW live_ticks AS
        SELECT * FROM market_ticks 
        ORDER BY rowid DESC 
        LIMIT 100;
    """
    )

    # Verify setup
    result = con.execute("SELECT '✅ Schema ready!' as status").fetchone()
    print(result[0])

    con.close()
    print("✅ TASK 2 DONE")


if __name__ == "__main__":
    setup_duckdb_schema()
