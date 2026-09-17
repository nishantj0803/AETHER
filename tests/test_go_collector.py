import os
import json
import pytest
from benchmarks.collector_benchmark import CollectorBenchmarkRunner, generate_telemetry_batch

GO_COLLECTOR_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "services",
    "go_collector"
)

def test_go_collector_file_structure():
    """Verify all required Go collector components exist."""
    required_files = [
        "go.mod",
        "types.go",
        "dedup.go",
        "db.go",
        "metrics.go",
        "consumer.go",
        "main.go",
        "Dockerfile",
    ]
    for filename in required_files:
        filepath = os.path.join(GO_COLLECTOR_DIR, filename)
        assert os.path.isfile(filepath), f"Missing required file: {filepath}"
        assert os.path.getsize(filepath) > 0, f"File is empty: {filepath}"

def test_telemetry_schema_compatibility():
    """Ensure generated telemetry records adhere to the schema expected by Go and Postgres."""
    records = generate_telemetry_batch(10)
    assert len(records) == 10

    sample = records[0]
    required_keys = ["event_id", "timestamp", "service_name", "level", "message", "http_status", "duration_ms", "metadata"]
    for key in required_keys:
        assert key in sample, f"Missing key '{key}' in generated telemetry batch"

    # Verify JSON serializability
    serialized = json.dumps(sample)
    unmarshaled = json.loads(serialized)
    assert unmarshaled["event_id"] == sample["event_id"]
    assert unmarshaled["service_name"] == "payment-service"

def test_collector_benchmark_runner_execution():
    """Test benchmark harness completes successfully and writes comparison report."""
    runner = CollectorBenchmarkRunner(record_count=100, batch_size=25)
    report = runner.run()

    assert "# 🏎️ Telemetry Ingestion Collector Benchmark: Python vs. Go" in report
    assert "Peak Throughput" in report
    assert "Memory Footprint (RSS)" in report

    report_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "benchmarks",
        "results",
        "collector_comparison.md"
    )
    assert os.path.isfile(report_path)
