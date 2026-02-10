#!/bin/bash
# Setup local Nexmark environment using Kind
# Usage: ./setup.sh [--nuke] [--quick] [--config CONFIG]
#
# Options:
#   --nuke           Delete existing cluster first (full rebuild)
#   --quick          Rebuild image + restart Flink only (skip cluster/operator setup)
#   --config CONFIG  Config file to use (default: nexmark-hudi-local.yaml)
#
# Examples:
#   ./setup.sh                                    # Create/update cluster with Hudi config
#   ./setup.sh --config nexmark-blackhole.yaml    # Use blackhole sink config
#   ./setup.sh --nuke                             # Delete and recreate from scratch
#   ./setup.sh --quick                            # Rebuild image and restart Flink
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"

NUKE=false
QUICK=false
CONFIG="nexmark-hudi-local.yaml"
CLUSTER_NAME="nexmark"
IMAGE_NAME="nexmark-k8s:local"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --nuke)
            NUKE=true
            shift
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --config)
            CONFIG="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./setup.sh [--nuke] [--quick] [--config CONFIG]"
            exit 1
            ;;
    esac
done

CONFIG_PATH="$REPO_ROOT/configs/$CONFIG"

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker not found"; exit 1; }
command -v kind >/dev/null 2>&1 || { echo "Error: kind not found. Install: brew install kind"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "Error: kubectl not found"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "Error: helm not found. Install: brew install helm"; exit 1; }

# Check for config file
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Config file not found: $CONFIG_PATH"
    echo "Available configs:"
    ls "$REPO_ROOT/configs/"*.yaml
    exit 1
fi

# Function to wait for Flink pods
wait_for_flink() {
    echo ""
    echo ">>> Waiting for Flink to be ready..."
    for i in {1..30}; do
        if kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session; then
            break
        fi
        echo "  Waiting for pod creation... ($i/30)"
        sleep 2
    done
    kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s
    kubectl wait --for=condition=Ready pod -l component=taskmanager -n flink-nexmark --timeout=120s 2>/dev/null || true
}

# Quick mode: rebuild image + restart Flink only
if [ "$QUICK" = true ]; then
    echo "=== Quick Redeploy (image + Flink restart) ==="
    echo ""

    # Build Docker image
    echo ">>> Building Docker image..."
    cd "$REPO_ROOT"
    docker build -t "$IMAGE_NAME" -f docker/Dockerfile \
        --build-arg NEXMARK_CONFIG_FILE="$CONFIG" .
    cd "$SCRIPT_DIR"

    # Load image into Kind
    echo ""
    echo ">>> Loading image into Kind cluster..."
    kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

    # Update ConfigMap
    echo ""
    echo ">>> Updating ConfigMap from $CONFIG..."
    kubectl create configmap nexmark-config \
        --from-file=nexmark.yaml="$CONFIG_PATH" \
        -n flink-nexmark \
        --dry-run=client -o yaml | kubectl apply -f -

    # Ensure metrics service exists and restart Flink deployment
    echo ""
    echo ">>> Applying metrics service..."
    kubectl apply -f nexmark-metrics-service.yaml

    echo ""
    echo ">>> Reapplying FlinkDeployment..."
    kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
    sleep 2
    kubectl apply -f flink-session.yaml

    wait_for_flink

    echo ""
    echo "=== Quick Redeploy Complete ==="
    echo ""
    kubectl get pods -n flink-nexmark
    echo ""
    echo "Run benchmark:"
    echo "  ./run.sh q0                          # Uses config from ConfigMap"
    echo "  ./run.sh q0 --sink hudi --storage minio --path-prefix s3a://nexmark"
    exit 0
fi

# Full setup mode
echo "=== Local Nexmark Setup ==="
echo "Config: $CONFIG"
echo ""

# Step 1: Handle existing cluster
if kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    if [ "$NUKE" = true ]; then
        echo ">>> Deleting existing cluster..."
        kind delete cluster --name "$CLUSTER_NAME"
    else
        echo ">>> Cluster '$CLUSTER_NAME' already exists (use --nuke to recreate)"
    fi
fi

# Step 2: Create cluster if needed
if ! kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo ">>> Creating Kind cluster..."
    kind create cluster --config kind-config.yaml
fi

# Step 3: Build Docker image
echo ""
echo ">>> Building Docker image..."
cd "$REPO_ROOT"
docker build -t "$IMAGE_NAME" -f docker/Dockerfile \
    --build-arg NEXMARK_CONFIG_FILE="$CONFIG" .
cd "$SCRIPT_DIR"

# Step 4: Load image into Kind
echo ""
echo ">>> Loading image into Kind cluster..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

# Step 5: Install Metrics Server (for kubectl top / resource monitoring)
if ! kubectl get deployment metrics-server -n kube-system &>/dev/null; then
    echo ""
    echo ">>> Installing Metrics Server..."
    kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
    kubectl patch deployment metrics-server -n kube-system --type='json' -p='[
      {"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls"},
      {"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-preferred-address-types=InternalIP"}
    ]' 2>/dev/null || true
    echo "  Waiting for metrics-server to be ready..."
    kubectl wait --for=condition=Available deployment metrics-server -n kube-system --timeout=120s 2>/dev/null || echo "  (metrics-server may take a minute to start)"
else
    echo ""
    echo ">>> Metrics Server already installed"
fi

# Step 6: Install cert-manager (if not present)
if ! kubectl get namespace cert-manager &>/dev/null; then
    echo ""
    echo ">>> Installing cert-manager..."
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml
    kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=300s
else
    echo ""
    echo ">>> cert-manager already installed"
fi

# Step 7: Install Flink Operator (if not present)
echo ""
echo ">>> Installing Flink Kubernetes Operator..."
kubectl create namespace flink-nexmark --dry-run=client -o yaml | kubectl apply -f -
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
helm repo update
helm upgrade --install flink-operator flink-operator/flink-kubernetes-operator \
    -n flink-nexmark --wait --timeout 5m

# Step 8: Deploy MinIO
echo ""
echo ">>> Deploying MinIO..."
kubectl apply -f minio.yaml
kubectl wait --for=condition=Ready pod -l app=minio -n flink-nexmark --timeout=120s
kubectl wait --for=condition=Complete job/minio-create-bucket -n flink-nexmark --timeout=60s 2>/dev/null || true

# Step 9: Create ConfigMap from config file
echo ""
echo ">>> Creating ConfigMap from $CONFIG..."
kubectl create configmap nexmark-config \
    --from-file=nexmark.yaml="$CONFIG_PATH" \
    -n flink-nexmark \
    --dry-run=client -o yaml | kubectl apply -f -

# Step 10: Setup RBAC
echo ""
echo ">>> Setting up RBAC..."
kubectl delete rolebinding flink-role-binding -n flink-nexmark 2>/dev/null || true
kubectl delete role flink-role -n flink-nexmark 2>/dev/null || true
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink-role
  namespace: flink-nexmark
rules:
- apiGroups: [""]
  resources: ["services", "pods", "configmaps"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: flink-role-binding
  namespace: flink-nexmark
subjects:
- kind: ServiceAccount
  name: flink
  namespace: flink-nexmark
roleRef:
  kind: Role
  name: flink-role
  apiGroup: rbac.authorization.k8s.io
EOF

# Step 11: Deploy metrics service and Flink session cluster
echo ""
echo ">>> Deploying metrics service..."
kubectl apply -f nexmark-metrics-service.yaml

echo ""
echo ">>> Deploying Flink session cluster..."
kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
sleep 2
kubectl apply -f flink-session.yaml

wait_for_flink

echo ""
echo "=== Setup Complete ==="
echo ""
kubectl get pods -n flink-nexmark
echo ""
echo "Run benchmark:"
echo "  ./run.sh q0                          # Uses config from ConfigMap"
echo "  ./run.sh q0 --sink hudi --storage minio --path-prefix s3a://nexmark"
echo ""
echo "Flink UI: kubectl port-forward svc/flink-session-rest 8081:8081 -n flink-nexmark"
echo "MinIO:    kubectl port-forward svc/minio 9001:9001 -n flink-nexmark"
