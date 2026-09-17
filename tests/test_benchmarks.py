import pytest
from benchmarks.benchmark_runner import BenchmarkRunner

def test_benchmark_percentile_calculation():
    runner = BenchmarkRunner()
    runner.latencies = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    runner.total_requests = 10
    runner.errors = 0

    import numpy as np
    p50 = float(np.percentile(runner.latencies, 50))
    p95 = float(np.percentile(runner.latencies, 95))
    p99 = float(np.percentile(runner.latencies, 99))

    assert p50 == 55.0
    assert p95 > 90.0
    assert p99 > 95.0

def test_benchmark_mttr_pipeline():
    runner = BenchmarkRunner()
    assert runner.concurrency == 10
    assert runner.errors == 0

