# nexmark-runner

Run [Nexmark](https://github.com/nexmark/nexmark) streaming benchmarks on Apache Flink with pluggable sinks (Hudi, blackhole).

## Prerequisites

```bash
# Check prerequisites and see install commands
make pre-setup
```

## Local Development (Kind + MinIO)

```bash
# Setup cluster
make setup-local

# Run benchmark
make run-local QUERY=q0

# Update config + restart Flink
make redeploy-local

# Rebuild image + update config + restart
make redeploy-local REBUILD=1

# Force full rebuild (no Docker cache)
make redeploy-local REBUILD=1 NO_CACHE=1

# Restart Flink pods only
make restart-local

# View MinIO console (minioadmin/minioadmin)
kubectl port-forward svc/minio 9001:9001 -n flink-nexmark

# Teardown
make teardown-local
```

## GCP GKE Deployment

```bash
# 1. Configure
cd k8s/gcp && cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project settings

# 2. Setup cluster
make setup-k8s

# 3. Run benchmark
make run-k8s QUERY=q0 SINK=hudi BUCKET=your-bucket

# Update config + restart Flink
make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=your-project

# Rebuild image + push + restart
make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=your-project REBUILD=1

# Force full rebuild (no Docker cache)
make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=your-project REBUILD=1 NO_CACHE=1

# Restart Flink pods only
make restart-k8s CONFIG=hudi-gcs PROJECT_ID=your-project

# 4. Teardown
make teardown-k8s
```

## Build Commands

```bash
# Build for different configs
make build CONFIG=blackhole      # flink1.18-blackhole
make build CONFIG=hudi-local     # flink1.18-hudi-local-1.1.1
make build CONFIG=hudi-gcs       # flink1.18-hudi-gcs-1.1.1

# Build without Docker cache
make build CONFIG=hudi-local NO_CACHE=1

# Push to GCR
make push CONFIG=hudi-gcs PROJECT_ID=your-project

# Show config info
make info CONFIG=hudi-gcs
```

## Queries

All Nexmark queries (q0-q22) are supported. Common ones:

| Query | Description |
|-------|-------------|
| q0 | Pass-through |
| q5 | Hot items (HOP window) |
| q7 | Highest bid (TUMBLE window) |

## License

Apache License 2.0
