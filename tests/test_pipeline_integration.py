import subprocess
import duckdb
import os
import time
from pathlib import Path
import pytest
import docker
import socket

PROJECT_ROOT = Path(__file__).resolve().parents[1]

@pytest.mark.integration
def test_pipeline_smoke(tmp_path):
    """Test pipeline with Kafka already running."""
    db_path = tmp_path / "test_market.duckdb"
    topic = f"test-smoke-{int(time.time())}"
    
    env = os.environ.copy()
    env["MARKET_DB_PATH"] = str(db_path)
    env["KAFKA_TOPIC"] = topic
    
    result = subprocess.run(
        ["./scripts/run_pipeline.sh"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    time.sleep(10)  # ↑ 10s wait for consumer
    assert "SUCCESS" in result.stdout
   
    conn = duckdb.connect(str(db_path))
    try:
        n_rows = conn.execute("SELECT COUNT(*) FROM market_ticks").fetchone()[0]
    finally:
        conn.close()
    
    # After subprocess.run() add:
    print("🖥️  PIPELINE STDOUT:", result.stdout)
    time.sleep(3)  # Wait for consumer
    print(f"📊 DB rows: {n_rows}")

    assert n_rows == 150
    print(f"✅ test_pipeline_smoke: {n_rows} rows")

@pytest.mark.integration
def test_pipeline_kafka_autolaunch(tmp_path):
    """Test pipeline auto-starts Kafka Docker container if needed."""
    db_path = tmp_path / "test_market_kafka.duckdb"
    topic = f"test-autolaunch-{int(time.time())}"
    
    env = os.environ.copy()
    env["MARKET_DB_PATH"] = str(db_path)
    env["KAFKA_TOPIC"] = topic
    
    # Stop Kafka
    client = docker.from_env()
    containers = client.containers.list(filters={"name": "kafka"})
    for container in containers:
        if container.status == 'running':
            container.stop(timeout=10)
            print("🛑 Stopped kafka container")
    
    # Verify port closed
    time.sleep(10) # ↑ Give Kafka time to stabilize
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 9092))
    sock.close()
    assert result != 0, "Kafka port still open!"
    
    print(f"🧪 Running pipeline with topic '{topic}' → DB '{db_path}'")
    
    result = subprocess.run(
        ["./scripts/run_pipeline.sh"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,  # 2min timeout
    )
    time.sleep(10)  # ↑ 10s wait for consumer
    print("🖥️  PIPELINE STDOUT:", result.stdout)
    print("🖥️  PIPELINE STDERR:", result.stderr)
    assert "SUCCESS" in result.stdout
    
    # DEBUG: Wait + check
    print("⏳ Waiting for consumer to finish...")
    time.sleep(5)
    
    assert db_path.exists(), "DB file not created!"
    
    conn = duckdb.connect(str(db_path))
    try:
        n_rows = conn.execute("SELECT COUNT(*) FROM market_ticks").fetchone()[0]
        print(f"📊 DB rows: {n_rows}")
    finally:
        conn.close()
    
    assert n_rows == 150, f"Expected 150 rows, got {n_rows}"
