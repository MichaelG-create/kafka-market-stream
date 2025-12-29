# src/market_streaming/cli_helpers.py

from market_streaming.application.ports import RunMetrics


def format_run_metrics(metrics: RunMetrics) -> str:
    """Return a human-readable string summary of run metrics for CLI output."""
    lines = [
        "",
        "📊 Run summary (US4):",
        f"  run_started_at:     {metrics.run_started_at}",
        f"  run_ended_at:       {metrics.run_ended_at}",
        f"  elapsed_seconds:    {metrics.elapsed_seconds:.2f}",
        f"  messages_processed: {metrics.messages_processed}",
        f"  errors:             {metrics.errors}",
        f"  max_timestamp:      {metrics.max_timestamp}",
    ]
    return "\n".join(lines)


def print_run_metrics(metrics: RunMetrics) -> None:
    """Print run metrics to stdout for interactive use."""
    print(format_run_metrics(metrics))
