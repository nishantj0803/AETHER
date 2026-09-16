.PHONY: help dev-up dev-down logs ps test traffic fault-bad-deploy fault-memory-leak fault-db-starve fault-reset

help:
	@echo "Aether - Autonomous SRE & Semantic Observability Engine"
	@echo ""
	@echo "Local Infrastructure Commands:"
	@echo "  make dev-up            Start Redpanda, Postgres+pgvector, Prometheus, Grafana, Demo Service"
	@echo "  make dev-down          Stop all containers and purge ephemeral volumes"
	@echo "  make ps                Show running containers status"
	@echo "  make logs              Follow container logs"
	@echo ""
	@echo "Traffic & Fault Injection:"
	@echo "  make traffic           Generate steady checkout traffic (5 concurrent workers)"
	@echo "  make fault-bad-deploy  Trigger bad deployment regression (25% 5xx rate)"
	@echo "  make fault-memory-leak Trigger memory leak fault"
	@echo "  make fault-db-starve   Trigger DB connection pool exhaustion"
	@echo "  make fault-reset       Clear all injected faults and restore v1.0.0"
	@echo ""
	@echo "Testing:"
	@echo "  make test              Run unit and integration test suite"

dev-up:
	docker compose up -d --build

dev-down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

traffic:
	python3 scripts/traffic_generator.py --url http://localhost:8000 --concurrency 5

fault-bad-deploy:
	curl -X POST http://localhost:8000/api/v1/admin/faults/inject \
		-H "Content-Type: application/json" \
		-d '{"fault_type": "bad_deployment"}'

fault-memory-leak:
	curl -X POST http://localhost:8000/api/v1/admin/faults/inject \
		-H "Content-Type: application/json" \
		-d '{"fault_type": "memory_leak", "parameters": {"chunk_mb": 50}}'

fault-db-starve:
	curl -X POST http://localhost:8000/api/v1/admin/faults/inject \
		-H "Content-Type: application/json" \
		-d '{"fault_type": "db_starve", "parameters": {"starve_count": 18}}'

fault-reset:
	curl -X POST http://localhost:8000/api/v1/admin/faults/reset

test:
	pytest tests/ -v
