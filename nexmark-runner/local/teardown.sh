#!/bin/bash
# Teardown local Nexmark environment
# Usage: ./teardown.sh
set -e

CLUSTER_NAME="nexmark"

echo "=== Tearing Down Local Nexmark Environment ==="
echo ""

# Delete Kind cluster
if kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo ">>> Deleting Kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME"
    echo "Cluster deleted."
else
    echo ">>> Cluster '$CLUSTER_NAME' not found (already deleted?)"
fi

echo ""
echo "=== Teardown Complete ==="
