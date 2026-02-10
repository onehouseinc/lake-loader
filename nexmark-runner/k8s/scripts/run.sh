#!/bin/bash
# Run Nexmark benchmark on GCP GKE Flink session cluster
# Uses Nexmark's native pluggable sink system via CLI arguments
#
# Usage: run.sh [queries] [--sink TYPE] [--bucket NAME] [-D key=value]
#
# Examples:
#   ./run.sh all                                  # All queries (blackhole sink)
#   ./run.sh q0                                   # Single query (blackhole sink)
#   ./run.sh q0 --sink hudi --bucket my-bucket    # Hudi sink to GCS
#   ./run.sh q0 --sink hudi --bucket b -D nexmark.sink.hudi.write.tasks=4
set -e

NAMESPACE="flink-nexmark"
QUERIES=""
SINK=""
BUCKET=""
EXTRA_ARGS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --sink)
            SINK="$2"
            shift 2
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        -D)
            EXTRA_ARGS+=("-D" "$2")
            shift 2
            ;;
        -D*)
            # Handle -Dkey=value (no space)
            EXTRA_ARGS+=("$1")
            shift
            ;;
        *)
            if [ -z "$QUERIES" ]; then
                QUERIES="$1"
            else
                # Pass through unknown args
                EXTRA_ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

QUERIES="${QUERIES:-all}"

echo "=== Nexmark Benchmark (GCP GKE) ==="
echo ""

# Check if session cluster exists
if ! kubectl get flinkdeployment flink-session -n "$NAMESPACE" &>/dev/null; then
    echo "Error: Flink session cluster not found."
    echo "Run setup.sh first to deploy the cluster."
    exit 1
fi

# Get JobManager pod name
POD=$(kubectl get pod -l app=flink-session,component=jobmanager -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "$POD" ]; then
    echo "Waiting for Flink JobManager pod..."
    kubectl wait --for=condition=Ready pod -l app=flink-session,component=jobmanager -n "$NAMESPACE" --timeout=120s
    POD=$(kubectl get pod -l app=flink-session,component=jobmanager -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
fi

echo "Using pod: $POD"
echo ""

# Build Nexmark CLI arguments
NEXMARK_ARGS=()

if [ -n "$SINK" ]; then
    NEXMARK_ARGS+=("--sink" "$SINK")

    # Determine storage type and path prefix based on sink
    if [ "$SINK" = "hudi" ]; then
        if [ -z "$BUCKET" ]; then
            echo "Error: --bucket is required when using --sink hudi"
            echo "Usage: ./run.sh q0 --sink hudi --bucket my-bucket"
            exit 1
        fi
        NEXMARK_ARGS+=("--storage" "gcs")
        NEXMARK_ARGS+=("--path-prefix" "gs://${BUCKET}")
        echo "Sink: hudi (gs://${BUCKET}/hudi/)"
    else
        echo "Sink: $SINK"
    fi
else
    echo "Sink: blackhole (default)"
fi

echo "Queries: $QUERIES"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    echo "Extra args: ${EXTRA_ARGS[*]}"
fi
echo ""

# Run Nexmark benchmark using native CLI
kubectl exec -t -n "$NAMESPACE" "$POD" -- \
    /opt/nexmark/bin/run_query.sh "$QUERIES" "${NEXMARK_ARGS[@]}" "${EXTRA_ARGS[@]}"

echo ""
if [ -n "$BUCKET" ]; then
    echo "Output location: gs://${BUCKET}/hudi/"
    echo "  gsutil ls gs://${BUCKET}/hudi/"
    echo ""
fi
echo "Flink UI: kubectl port-forward svc/flink-session-rest 8081:8081 -n $NAMESPACE"
