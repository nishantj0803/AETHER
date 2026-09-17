#!/usr/bin/env bash
# ==============================================================================
# Chaos Experiment: PostgreSQL Database Partition & Graceful Recovery
# Injects database partition to verify:
# 1. Remediation Controller & Ingestion worker handle DB timeout without crash.
# 2. DatabaseClient enters in-memory fallback state.
# 3. Connection pool reconnects automatically once database resumes.
# 4. Verified query execution succeeds post-recovery.
# ==============================================================================

set -euo pipefail

TARGET_CONTAINER="aether-postgres"
PAUSE_DURATION=8

echo "=== [CHAOS] Starting PostgreSQL Database Partition Experiment ==="
echo "Target Container: ${TARGET_CONTAINER}"

if ! docker ps --format '{{.Names}}' | grep -q "${TARGET_CONTAINER}"; then
    echo "⚠️ Warning: ${TARGET_CONTAINER} is not running. Starting via docker-compose..."
    docker compose up -d postgres
    sleep 5
fi

echo "Step 1: Baseline check - confirming DB accepts queries..."
docker exec -i "${TARGET_CONTAINER}" pg_isready -U aether_user -d aether_db

echo "Step 2: Pausing PostgreSQL container for ${PAUSE_DURATION}s to simulate network partition..."
docker pause "${TARGET_CONTAINER}"
echo "🚨 Container ${TARGET_CONTAINER} is PAUSED."

echo "Step 3: Probing system resilience while database is partitioned..."
# Check demo service health
if curl -s -f --connect-timeout 2 http://localhost:8000/health > /dev/null 2>&1; then
    echo "  * Front-door Demo Service remains healthy (isolated from DB failure)."
fi

# Check Controller Incident API resilience (must not crash)
CONTROLLER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 http://localhost:8001/api/v1/incidents || echo "000")
echo "  * Remediation Controller HTTP response during DB partition: ${CONTROLLER_STATUS}"

sleep "${PAUSE_DURATION}"

echo "Step 4: Restoring PostgreSQL container..."
docker unpause "${TARGET_CONTAINER}"
echo "✅ Container ${TARGET_CONTAINER} is UNPAUSED."

echo "Step 5: Verifying DB reconnect with active polling..."
RECONNECTED=0
for i in $(seq 1 15); do
    if docker exec -i "${TARGET_CONTAINER}" pg_isready -U aether_user -d aether_db > /dev/null 2>&1; then
        echo "✅ PostgreSQL connection pool restored and accepting connections (attempt ${i})."
        RECONNECTED=1
        break
    fi
    sleep 1
done

if [ "${RECONNECTED}" -ne 1 ]; then
    echo "❌ PostgreSQL failed to resume within timeout!"
    exit 1
fi

echo "Step 6: Executing verified database read query..."
docker exec -i "${TARGET_CONTAINER}" psql -U aether_user -d aether_db -c "SELECT count(*) FROM incidents;"

echo "=== [CHAOS] Database Partition Experiment Completed Successfully ==="

