#!/bin/bash
# Teardown Nexmark benchmark environment on GCP GKE
# Usage: ./teardown.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

INFRA_DIR="$PROJECT_ROOT/k8s/gcp"

echo "=== Nexmark Benchmark Teardown (GCP GKE) ==="
echo ""
echo "This will destroy all resources:"
echo "  - GKE cluster"
echo "  - GCS bucket (and all data)"
echo "  - Service accounts"
echo "  - VPC and networking"
echo "  - Artifact Registry repository"
echo ""

read -p "Are you sure you want to continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Teardown cancelled."
    exit 0
fi

# Delete Kubernetes resources first (optional, Terraform will handle this)
echo ""
echo "=== Deleting Kubernetes resources ==="
kubectl delete flinkdeployment --all -n flink-nexmark 2>/dev/null || true
kubectl delete flinksessionjob --all -n flink-nexmark 2>/dev/null || true

# Run Terraform destroy
echo ""
echo "=== Running Terraform destroy ==="
cd "$INFRA_DIR"
terraform destroy

echo ""
echo "=== Teardown Complete ==="
echo ""
echo "All resources have been destroyed."
