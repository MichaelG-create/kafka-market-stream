## Future Monitoring

This project is designed so that a future Grafana dashboard can monitor the Kafka → DuckDB pipeline using the `pipeline_metrics` table as a data source. That table already contains per‑run metrics such as `messages_processed`, `errors`, `elapsed_seconds`, and `max_timestamp` (event‑time).

A simple “Grafana‑style” dashboard could include at least the following panels:

- **Messages per second (throughput)**  
  - Data source: `pipeline_metrics`  
  - Metric: `messages_processed / elapsed_seconds` per run, plotted over time as a time‑series panel.  
  - Purpose: Show how many messages the consumer processes per second and reveal drops in throughput.

- **Error rate over time**  
  - Data source: `pipeline_metrics`  
  - Metric: `errors / (messages_processed + errors)` per run, shown as a percentage over time.  
  - Purpose: Track pipeline reliability and quickly spot spikes in failures.

- **Run duration**  
  - Data source: `pipeline_metrics`  
  - Metric: `elapsed_seconds` per run, displayed as a time‑series or bar chart.  
  - Purpose: Monitor how long each run takes and detect performance regressions.

- **Data latency / freshness**  
  - Data source: `pipeline_metrics`  
  - Metric: time difference between `run_ended_at` and `max_timestamp` (for example, plotted as seconds per run).  
  - Purpose: Indicate how “real‑time” the pipeline is by measuring end‑to‑end latency from event time to processed time.

With this plan in place, plugging Grafana (or a similar tool) on top of `pipeline_metrics` later will provide a clear, honest monitoring story around latency, throughput, and error rate.

