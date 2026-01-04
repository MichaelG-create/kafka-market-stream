# kafka-market-stream
[![CI](https://github.com/MichaelG-create/kafka-market-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/MichaelG-create/kafka-market-stream/actions/workflows/ci.yml)

`kafka-market-stream` is an end‑to‑end streaming prototype that ingests synthetic market index data into Kafka, persists it to DuckDB, and serves live Grafana dashboards over a small Flask API.

---

## Overview

The project simulates a real‑time **market indices** feed (SP500, STOXX600, NIKKEI225) and demonstrates a complete path:

![kafka-market-stream workflow](docs/excalidraw-diags/kafka-grafana-workflow.png)

It is structured as a Python package with separate **domain**, **application services**, **infrastructure adapters**, and a small **API + dashboards** layer, plus CI smoke tests on GitHub Actions.

---

## How to run the stack

This section starts the full stack locally: Kafka, the Python pipeline, DuckDB, and the Flask API used by Grafana.

1. **Install dependencies**

   ```bash
   pip install uv          # or: pipx install uv
   uv sync                 # create .venv and install all deps
   ```

2. **Generate sample market data**

   ```bash
   python data/generate_sample_data.py
   ```

   This creates `data/indices_sample.csv` with 150 timestamped rows (50 per symbol).

3. **Run the pipeline (Kafka + producer + consumer)**

   With Docker available, use the helper script:

   ```bash
   ./scripts/run_pipeline.sh
   ```

   This will:

   - Start an `apache/kafka:latest` container on `localhost:9092`.
   - Run the producer (`market_streaming.main_producer`) and consumer (`market_streaming.main_consumer`).
   - Populate `data/market_data.duckdb` with market ticks and write run metrics + logs.

4. **Start the Flask metrics API**

   ```bash
   uv run python api/duckdb-api.py
   ```

   The API listens on `http://localhost:8080` and exposes:

   - `GET /metrics` – aggregates per symbol from DuckDB.
   - `GET /live` – recent ticks reshaped as a wide time series.

---

## Grafana dashboards (Infinity datasource)
![kafka-market-stream dashboard](docs/kafka-grafana-dashboard.png)

Grafana reads directly from the Flask API using the **Yesoreyeram Infinity** datasource.

- **Live prices and volumes**

  - Endpoint: `GET http://localhost:8080/live`  
  - Returns JSON with:
    - `time`
    - `SP500`, `STOXX600`, `NIKKEI225`
    - `SP500volume`, `STOXX600volume`, `NIKKEI225volume`
  - Used by:
    - “Live Price Trends – Global Markets” timeseries panel  
    - “Trading Volume” stacked area/column panel  
  - Typical refresh interval: **5s** in the dashboard JSON.

- **Aggregated market metrics**

  - Endpoint: `GET http://localhost:8080/metrics`  
  - Returns JSON with:
    - `symbol`, `tickcount`, `avgprice`, `maxprice`, `minprice`
  - Used by:
    - “Market Metrics” table  
    - “Market Statistics Summary” table with gradient colouring

The Infinity queries and panels are defined in:

- `data/grafanadashboard1settings.json`
- `data/grafanadashboard2settings.json`

These can be imported into any Grafana instance without manual reconfiguration.

To run Grafana locally with the pre‑wired dashboards:

```bash
docker compose -f docker-compose.grafana.yml up --build
```

This starts:

- `duckdb-api` on port `8080` (used by Infinity)
- `grafana` on port `3000` with the dashboards provisioned from the JSON files

---

## Project structure

Key directories and files:

- `src/market_streaming/`
  - `application/` – producer and consumer services (`MarketTickProducerService`, `MarketTickConsumerService`)
  - `domain/` – `MarketTick` and `RunMetrics` dataclasses
  - `infrastructure/` – Kafka producer/consumer, DuckDB repositories, JSON file logger
  - `main_producer.py` / `main_consumer.py` – wire everything together
- `api/duckdb-api.py` – Flask API exposing `/metrics` and `/live`
- `data/generate_sample_data.py` – create `indices_sample.csv`
- `data/duckdb_once_setup_grafana.py` – add `timestampreceived`, `marketmetrics` and `liveticks` views
- `data/grafanadashboard1settings.json`, `data/grafanadashboard2settings.json` – dashboard definitions
- `tests/test_pipeline_integration.py` – smoke tests that run the pipeline and assert 150 rows in DuckDB
- `.github/workflows/ci.yml` – CI: ruff, black, pylint, and smoke tests against a Kafka service

---

## Monitoring & CI

The pipeline records both **run-level metrics** and **structured logs** to support monitoring.

- `DuckDBMetricsRepository` writes `RunMetrics` entries into `pipeline_metrics` in DuckDB.
- `JsonFileLogger` emits JSON logs to `logs/pipeline.log` for pipeline start, end and errors.
- GitHub Actions workflow runs linting and `pytest tests/test_pipeline_integration.py::test_pipeline_smoke` against a Kafka container on every push.
