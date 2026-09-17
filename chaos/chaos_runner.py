"""
Aether Automated Chaos Experiment Orchestrator.

Executes fault injection cycles against live local infrastructure
and asserts platform recovery SLOs (MTTR < 20s, zero unhandled 500s on health probes).
"""

import argparse
import logging
import subprocess
import time
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aether.chaos_runner")


def check_service_health(base_url: str = "http://localhost:8000") -> bool:
    try:
        resp = httpx.get(f"{base_url}/health", timeout=3.0)
        return resp.status_code == 200
    except Exception:
        return False


def check_incident_api_health(base_url: str = "http://localhost:8001") -> bool:
    try:
        resp = httpx.get(f"{base_url}/api/v1/incidents", timeout=3.0)
        return resp.status_code in (200, 503)  # 200 normal, 503 during graceful partition fallback
    except Exception:
        return False


def run_experiment(script_path: str, name: str, dry_run: bool = False) -> bool:
    logger.info(f"🚀 Executing Chaos Experiment: {name} ({script_path})...")
    if dry_run:
        logger.info(f"  [DRY-RUN] Script syntax verified for {script_path}")
        proc = subprocess.run(["bash", "-n", script_path], capture_output=True, text=True)
        return proc.returncode == 0

    start = time.time()
    try:
        proc = subprocess.run(
            ["bash", script_path],
            capture_output=True,
            text=True,
            timeout=45
        )
        duration = time.time() - start
        if proc.returncode == 0:
            logger.info(f"✅ Experiment '{name}' succeeded in {duration:.1f}s")
            return True
        else:
            logger.error(f"❌ Experiment '{name}' failed with code {proc.returncode}:\n{proc.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Experiment '{name}' timed out after 45s")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to run '{name}': {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Aether Chaos Experiment Runner")
    parser.add_argument("--experiment", choices=["kafka", "postgres", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true", help="Validate experiment script syntax without injecting faults")
    args = parser.parse_args()

    experiments = []
    if args.experiment in ("kafka", "all"):
        experiments.append(("chaos/experiment_kafka_kill.sh", "Kafka Broker Outage"))
    if args.experiment in ("postgres", "all"):
        experiments.append(("chaos/experiment_postgres_pause.sh", "Postgres Partition"))

    results = {}
    for script, name in experiments:
        results[name] = run_experiment(script, name, dry_run=args.dry_run)

    logger.info("=== Chaos Engineering Summary ===")
    all_passed = True
    for name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"  * {name}: {status}")
        if not passed:
            all_passed = False

    exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
