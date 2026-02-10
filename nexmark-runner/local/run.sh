#!/bin/bash
# Run Nexmark benchmark locally
# Usage: ./run.sh [queries] [--sink TYPE] [--storage PROFILE] [--path-prefix PATH] [-D key=value]
#
# Examples:
#   ./run.sh q0                                              # Use config from ConfigMap
#   ./run.sh q0 --sink blackhole                             # Override to blackhole
#   ./run.sh q0 --sink hudi --storage minio --path-prefix s3a://nexmark
#   ./run.sh q0 --sink hudi -Dnexmark.sink.hudi.write.tasks=16
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

QUERIES=""
EXTRA_ARGS=()

# Parse arguments - collect query and pass through everything else
while [[ $# -gt 0 ]]; do
    case $1 in
        --sink|--storage|--path-prefix|-D*)
            EXTRA_ARGS+=("$1")
            if [[ "$1" != -D* ]]; then
                shift
                EXTRA_ARGS+=("$1")
            fi
            shift
            ;;
        *)
            if [ -z "$QUERIES" ]; then
                QUERIES="$1"
            else
                EXTRA_ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

QUERIES="${QUERIES:-q0}"

echo "=== Nexmark Benchmark (Local) ==="
echo ""

# Check if cluster is ready
kubectl get flinkdeployment flink-session -n flink-nexmark >/dev/null 2>&1 || {
    echo "Error: Flink session cluster not found. Run ./setup.sh first."
    exit 1
}

# Get JobManager pod name
POD=$(kubectl get pod -l component=jobmanager -n flink-nexmark -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "$POD" ]; then
    echo "Waiting for Flink JobManager pod..."
    kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=120s
    POD=$(kubectl get pod -l component=jobmanager -n flink-nexmark -o jsonpath='{.items[0].metadata.name}')
fi

echo "Using pod: $POD"
echo "Query: $QUERIES"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    echo "Extra args: ${EXTRA_ARGS[*]}"
fi
echo ""

# Run Nexmark benchmark using native runner
# Config is loaded from /opt/nexmark/conf/nexmark.yaml (mounted via ConfigMap)
# CLI args (--sink, --storage, --path-prefix, -D) override ConfigMap values
kubectl exec -t -n flink-nexmark "$POD" -- \
    /opt/nexmark/bin/run_query.sh "$QUERIES" "${EXTRA_ARGS[@]}"

echo ""
echo "Flink UI: kubectl port-forward svc/flink-session-rest 8081:8081 -n flink-nexmark"
echo "MinIO:    kubectl port-forward svc/minio 9001:9001 -n flink-nexmark"
