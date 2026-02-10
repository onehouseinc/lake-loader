#!/bin/bash
# Setup Nexmark benchmark environment on GCP GKE
# Usage: ./setup.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

INFRA_DIR="$PROJECT_ROOT/k8s/gcp"

echo "=== Nexmark Benchmark Setup (GCP GKE) ==="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
command -v terraform >/dev/null 2>&1 || { echo "Error: terraform not found"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "Error: kubectl not found"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Error: docker not found"; exit 1; }
command -v gcloud >/dev/null 2>&1 || { echo "Error: gcloud CLI not found"; exit 1; }

# Check for terraform.tfvars
if [ ! -f "$INFRA_DIR/terraform.tfvars" ]; then
    echo "Error: $INFRA_DIR/terraform.tfvars not found"
    echo "Please copy terraform.tfvars.example to terraform.tfvars and configure it"
    exit 1
fi

# Step 1: Apply Terraform
echo ""
echo "=== Step 1: Provisioning GKE cluster and GCS bucket ==="
cd "$INFRA_DIR"
terraform init
terraform apply

# Get GKE outputs
PROJECT_ID=$(terraform output -raw project_id)
REGION=$(terraform output -raw region)
ZONE=$(terraform output -raw zone)
CLUSTER_NAME=$(terraform output -raw cluster_name)
GCS_BUCKET=$(terraform output -raw gcs_bucket_name)
IMAGE_URL=$(terraform output -raw flink_image_url)

echo ""
echo "Project ID: $PROJECT_ID"
echo "GCS Bucket: $GCS_BUCKET"
echo "Image URL: $IMAGE_URL"

# Step 2: Configure kubectl
echo ""
echo "=== Step 2: Configuring kubectl ==="
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID"

# Wait for Flink operator to be ready
echo "Waiting for Flink Kubernetes Operator to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/flink-kubernetes-operator -n flink-nexmark || {
    echo "Warning: Flink operator not ready yet. Check with: kubectl get pods -n flink-nexmark"
}

# Step 3: Build and push Docker image
echo ""
echo "=== Step 3: Building and pushing Docker image ==="
cd "$PROJECT_ROOT"
make push CONFIG=hudi-gcs PROJECT_ID="$PROJECT_ID" REGION="$REGION"

# Step 4: Create ConfigMap from GCS config
echo ""
echo "=== Step 4: Creating ConfigMap ==="
kubectl create configmap nexmark-config \
    --from-file=nexmark.yaml="$PROJECT_ROOT/configs/nexmark-hudi-gcs.yaml" \
    -n flink-nexmark \
    --dry-run=client -o yaml | kubectl apply -f -

# Step 5: Deploy Flink session cluster and metrics service
echo ""
echo "=== Step 5: Deploying Flink session cluster ==="
kubectl apply -f "$PROJECT_ROOT/k8s/nexmark-metrics-service.yaml"
kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
sleep 2
# Patch FlinkDeployment with registry image URL
sed "s|image: nexmark-runner:.*|image: $IMAGE_URL|g" "$PROJECT_ROOT/k8s/flink-session.yaml" | kubectl apply -f -

echo ""
echo "Waiting for Flink pods to be ready..."
for i in $(seq 1 30); do
    if kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session; then
        break
    fi
    echo "  Waiting for pod creation... ($i/30)"
    sleep 2
done
kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s

echo ""
echo "=== Setup Complete ==="
echo ""
kubectl get pods -n flink-nexmark
echo ""
echo "GCS Bucket: gs://$GCS_BUCKET"
echo "Image URL: $IMAGE_URL"
echo ""
echo "Run benchmark:"
echo "  ./k8s/scripts/run.sh q0 --sink hudi --bucket $GCS_BUCKET"
echo ""
echo "Flink UI: kubectl port-forward svc/flink-session-rest 8081:8081 -n flink-nexmark"
