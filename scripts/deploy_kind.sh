#!/usr/bin/env bash
# ==============================================================================
# Aether Kind 3-Node Kubernetes Cluster Automated Deployer
# Builds local container images, provisions the Kind cluster, loads images,
# applies least-privilege RBAC + service manifests, and verifies rollouts.
# ==============================================================================

set -euo pipefail

CLUSTER_NAME="aether-cluster"
NAMESPACE="aether-system"

echo "=== [KIND] Starting Aether Kubernetes Cluster Deployment ==="

# 1. Dependency checks
command -v docker >/dev/null 2>&1 || { echo "❌ Error: docker is required but not installed."; exit 1; }
command -v kind >/dev/null 2>&1 || { echo "❌ Error: kind is required but not installed. Install via: brew install kind"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "❌ Error: kubectl is required but not installed. Install via: brew install kubectl"; exit 1; }

# 2. Cluster Creation
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "ℹ️ Kind cluster '${CLUSTER_NAME}' already exists."
else
    echo "🚀 Creating Kind 3-node cluster from deploy/k8s/kind-cluster.yaml..."
    kind create cluster --name "${CLUSTER_NAME}" --config deploy/k8s/kind-cluster.yaml
fi

# Switch kubectl context
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

# 3. Build Container Images Locally
echo "📦 Building local Docker images..."
docker build -t aether-demo-service:latest -f services/demo_service/Dockerfile .
docker build -t aether-remediation-controller:latest -f services/remediation_controller/Dockerfile .
docker build -t aether-go-collector:latest -f services/go_collector/Dockerfile services/go_collector

# 4. Load Images into Kind Cluster Nodes
echo "🚚 Loading Docker images into Kind worker nodes..."
kind load docker-image aether-demo-service:latest --name "${CLUSTER_NAME}"
kind load docker-image aether-remediation-controller:latest --name "${CLUSTER_NAME}"
kind load docker-image aether-go-collector:latest --name "${CLUSTER_NAME}"

# 5. Apply Kubernetes Manifests in Sequential Dependency Order
echo "📄 Applying Kubernetes manifests..."
kubectl apply -f deploy/k8s/00-namespace.yaml
kubectl apply -f deploy/k8s/01-rbac.yaml
kubectl apply -f deploy/k8s/02-payment-service.yaml
kubectl apply -f deploy/k8s/03-aether-controller.yaml
kubectl apply -f deploy/k8s/04-go-collector.yaml

# 6. Wait for Deployment Rollouts
echo "⏳ Waiting for pod rollouts in namespace '${NAMESPACE}'..."
kubectl rollout status deployment/payment-service -n "${NAMESPACE}" --timeout=90s
kubectl rollout status deployment/aether-remediation-controller -n "${NAMESPACE}" --timeout=90s
kubectl rollout status deployment/aether-go-collector -n "${NAMESPACE}" --timeout=90s

# 7. Print Final Cluster Topology
echo "=== Cluster Deployment Succeeded ==="
kubectl get pods -n "${NAMESPACE}" -o wide
echo ""
kubectl get svc -n "${NAMESPACE}"
echo ""
echo "Access endpoints via mapped host ports:"
echo "  * Payment Service: http://localhost:8000"
echo "  * Prometheus:      http://localhost:9090"
