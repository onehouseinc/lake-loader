# Nexmark Docker Build Makefile
# Usage:
#   make build CONFIG=hudi-gcs
#   make build CONFIG=hudi-local
#   make build CONFIG=blackhole
#   make push CONFIG=hudi-gcs PROJECT_ID=your-project

.PHONY: help pre-setup build push info build-amd64 setup-k8s teardown-k8s run-k8s redeploy-k8s restart-k8s setup-local teardown-local run-local redeploy-local restart-local

# Configuration
IMAGE_NAME ?= nexmark-runner
HUDI_VERSION ?= 1.1.1
FLINK_VERSION ?= 1.18
PROJECT_ID ?=
REGION ?= us-central1

# Derived paths
CONFIGS_DIR = configs

# Derived from CONFIG
# For blackhole, no config file needed (uses nexmark defaults)
NEXMARK_CONFIG_FILE = $(if $(filter blackhole,$(CONFIG)),,nexmark-$(CONFIG).yaml)
# Image tag format: flink<version>-<config>[-<hudi_version>]
# blackhole doesn't use Hudi, so skip Hudi version suffix for it
IMAGE_TAG = $(if $(filter blackhole,$(CONFIG)),flink$(FLINK_VERSION)-$(CONFIG),flink$(FLINK_VERSION)-$(CONFIG)-$(HUDI_VERSION))

# Default target
help:
	@echo "Nexmark Docker Build"
	@echo ""
	@echo "Usage:"
	@echo "  make build CONFIG=<config>                    Build Docker image"
	@echo "  make push CONFIG=<config> PROJECT_ID=<id>     Build and push to GCR"
	@echo ""
	@echo "Available configs:"
	@ls -1 $(CONFIGS_DIR)/*.yaml 2>/dev/null | xargs -I{} basename {} .yaml | sed 's/^nexmark-/  /'
	@echo ""
	@echo "Examples:"
	@echo "  make build CONFIG=hudi-gcs"
	@echo "  make build CONFIG=hudi-local"
	@echo "  make build CONFIG=blackhole"
	@echo "  make push CONFIG=hudi-gcs PROJECT_ID=my-project"
	@echo ""
	@echo "Other targets:"
	@echo "  make pre-setup          Check prerequisites and show install commands"
	@echo "  make info CONFIG=<cfg>  Show build configuration"
	@echo ""
	@echo "K8s targets (GKE):"
	@echo "  make setup-k8s          Setup GKE cluster (terraform + k8s)"
	@echo "  make teardown-k8s       Teardown GKE cluster"
	@echo "  make run-k8s QUERY=q0 [SINK=hudi] [BUCKET=name]"
	@echo "  make restart-k8s        Restart Flink pods (after Ctrl+C)"
	@echo "  make redeploy-k8s CONFIG=hudi-gcs  Update config + restart"
	@echo "  make redeploy-k8s CONFIG=hudi-gcs REBUILD=1 PROJECT_ID=<id>  Rebuild + push + restart"
	@echo ""
	@echo "Local targets (Kind + MinIO):"
	@echo "  make setup-local        Setup local Kind cluster"
	@echo "  make teardown-local     Teardown local cluster"
	@echo "  make run-local QUERY=q0 [ARGS='--sink hudi']"
	@echo "  make restart-local      Restart Flink pods (after Ctrl+C)"
	@echo "  make redeploy-local     Update config + restart Flink"
	@echo "  make redeploy-local REBUILD=1  Rebuild image + update + restart"
	@echo ""
	@echo "Configuration variables:"
	@echo "  IMAGE_NAME     Docker image name (default: nexmark-runner)"
	@echo "  HUDI_VERSION   Hudi version (default: 1.1.1)"
	@echo "  FLINK_VERSION  Flink version for Hudi bundle (default: 1.18)"
	@echo "  PROJECT_ID     GCP project ID (required for push)"
	@echo "  REGION         GCP region (default: us-central1)"

# Check prerequisites and show install commands
pre-setup:
	@echo "=== Checking Prerequisites ==="
	@echo ""
	@echo "Common (required for all):"
	@printf "  docker:    "; command -v docker >/dev/null 2>&1 && echo "OK" || echo "MISSING - https://docs.docker.com/get-docker/"
	@printf "  kubectl:   "; command -v kubectl >/dev/null 2>&1 && echo "OK" || echo "MISSING - brew install kubectl"
	@echo ""
	@echo "Local development (Kind + MinIO):"
	@printf "  kind:      "; command -v kind >/dev/null 2>&1 && echo "OK" || echo "MISSING - brew install kind"
	@printf "  helm:      "; command -v helm >/dev/null 2>&1 && echo "OK" || echo "MISSING - brew install helm"
	@echo ""
	@echo "GKE deployment:"
	@printf "  gcloud:    "; command -v gcloud >/dev/null 2>&1 && echo "OK" || echo "MISSING - https://cloud.google.com/sdk/docs/install"
	@printf "  terraform: "; command -v terraform >/dev/null 2>&1 && echo "OK" || echo "MISSING - brew install terraform"

# Show configuration
info:
ifndef CONFIG
	@echo "Usage: make info CONFIG=hudi-gcs"
	@exit 1
endif
	@echo "Configuration:"
	@echo "  CONFIG:         $(CONFIG)"
	@echo "  IMAGE_NAME:     $(IMAGE_NAME)"
	@echo "  IMAGE_TAG:      $(IMAGE_TAG)"
	@echo "  NEXMARK_CONFIG_FILE: $(NEXMARK_CONFIG_FILE)"
	@echo "  HUDI_VERSION:   $(HUDI_VERSION)"
	@echo "  PROJECT_ID:     $(PROJECT_ID)"
	@echo "  REGION:         $(REGION)"

# Build target
# Usage: make build CONFIG=hudi-local [NO_CACHE=1]
build:
ifndef CONFIG
	@echo "Error: CONFIG not specified"
	@echo "Usage: make build CONFIG=hudi-gcs|hudi-local|blackhole"
	@exit 1
endif
	@echo "=== Building Nexmark Docker Image ==="
	@echo "Config:     $(CONFIG)"
	@echo "Image:      $(IMAGE_NAME):$(IMAGE_TAG)"
	@echo ""
	docker build \
		$(if $(NO_CACHE),--no-cache,) \
		--build-arg NEXMARK_CONFIG_FILE=$(NEXMARK_CONFIG_FILE) \
		--build-arg HUDI_VERSION=$(HUDI_VERSION) \
		-t "$(IMAGE_NAME):$(IMAGE_TAG)" \
		-f docker/Dockerfile .
	@echo ""
	@echo "=== Build Complete ==="
	@echo "Image: $(IMAGE_NAME):$(IMAGE_TAG)"
	@echo ""
	@echo "To load into Kind cluster:"
	@echo "  kind load docker-image $(IMAGE_NAME):$(IMAGE_TAG) --name nexmark"

# Push target
push: build
ifndef PROJECT_ID
	@echo "Error: PROJECT_ID not specified"
	@echo "Usage: make push CONFIG=hudi-gcs PROJECT_ID=your-project"
	@exit 1
endif
	@REGISTRY_URL="$(REGION)-docker.pkg.dev/$(PROJECT_ID)/flink"; \
	IMAGE_URL="$$REGISTRY_URL/$(IMAGE_NAME):$(IMAGE_TAG)"; \
	echo "=== Pushing to Artifact Registry ==="; \
	echo "Configuring Docker authentication..."; \
	gcloud auth configure-docker "$(REGION)-docker.pkg.dev" --quiet; \
	echo "Tagging image..."; \
	docker tag "$(IMAGE_NAME):$(IMAGE_TAG)" "$$IMAGE_URL"; \
	echo "Pushing to $$IMAGE_URL..."; \
	docker push "$$IMAGE_URL"; \
	echo ""; \
	echo "=== Push Complete ==="; \
	echo "Image: $$IMAGE_URL"

# Build AMD64 image for GKE (from Mac ARM)
# Usage: make build-amd64 CONFIG=hudi-gcs [NO_CACHE=1]
build-amd64:
ifndef CONFIG
	@echo "Error: CONFIG not specified"
	@echo "Usage: make build-amd64 CONFIG=hudi-gcs"
	@exit 1
endif
	@echo "=== Building AMD64 Image for GKE ==="
	docker build \
		--platform linux/amd64 \
		$(if $(NO_CACHE),--no-cache,) \
		--build-arg NEXMARK_CONFIG_FILE=$(NEXMARK_CONFIG_FILE) \
		--build-arg HUDI_VERSION=$(HUDI_VERSION) \
		-t "$(IMAGE_NAME):$(IMAGE_TAG)" \
		-f docker/Dockerfile .
	@echo ""
	@echo "=== Build Complete ==="
	@echo "Image: $(IMAGE_NAME):$(IMAGE_TAG) (linux/amd64)"

# K8s targets
setup-k8s:
	@echo "=== Setting up GKE environment ==="
	./k8s/scripts/setup.sh

teardown-k8s:
	@echo "=== Tearing down GKE environment ==="
	./k8s/scripts/teardown.sh

run-k8s:
ifndef QUERY
	@echo "Error: QUERY not specified"
	@echo "Usage: make run-k8s QUERY=q0 [SINK=hudi] [BUCKET=my-bucket]"
	@exit 1
endif
	@ARGS="$(QUERY)"; \
	if [ -n "$(SINK)" ]; then ARGS="$$ARGS --sink $(SINK)"; fi; \
	if [ -n "$(BUCKET)" ]; then ARGS="$$ARGS --bucket $(BUCKET)"; fi; \
	./k8s/scripts/run.sh $$ARGS

# Local targets (Kind + MinIO)
setup-local:
	@echo "=== Setting up local Kind environment ==="
	./local/setup.sh

teardown-local:
	@echo "=== Tearing down local environment ==="
	./local/teardown.sh

run-local:
ifndef QUERY
	@echo "Error: QUERY not specified"
	@echo "Usage: make run-local QUERY=q0 [ARGS='--sink hudi --storage minio']"
	@exit 1
endif
	./local/run.sh $(QUERY) $(ARGS)

# Restart Flink pods (use after Ctrl+C or stuck run)
restart-local:
	@echo "=== Restarting FlinkDeployment ==="
	kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
	sleep 2
	kubectl apply -f local/flink-session.yaml
	@echo "=== Waiting for Flink pods ==="
	@for i in $$(seq 1 30); do \
		kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session && break; \
		echo "  Waiting for pod creation... ($$i/30)"; \
		sleep 2; \
	done
	kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s
	@echo "=== Done. Run: make run-local QUERY=q0 ==="

# Redeploy: update config + restart Flink (or rebuild image if REBUILD=1)
# Usage:
#   make redeploy-local              # Update config + restart
#   make redeploy-local REBUILD=1    # Rebuild image + update config + restart
#   make redeploy-local REBUILD=1 NO_CACHE=1  # Force full rebuild
redeploy-local:
	$(eval CONFIG := $(or $(CONFIG),hudi-local))
ifdef REBUILD
	@$(MAKE) build CONFIG=$(CONFIG) $(if $(NO_CACHE),NO_CACHE=1,)
	@echo "=== Tagging and loading image into Kind ==="
	docker tag "$(IMAGE_NAME):$(IMAGE_TAG)" nexmark-k8s:local
	kind load docker-image nexmark-k8s:local --name nexmark
endif
	@echo "=== Updating ConfigMap ==="
	kubectl create configmap nexmark-config \
		--from-file=nexmark.yaml="configs/$(NEXMARK_CONFIG_FILE)" \
		-n flink-nexmark --dry-run=client -o yaml | kubectl apply -f -
	@echo "=== Applying metrics service ==="
	kubectl apply -f local/nexmark-metrics-service.yaml
	@echo "=== Restarting FlinkDeployment ==="
	kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
	sleep 2
	kubectl apply -f local/flink-session.yaml
	@echo "=== Waiting for Flink pods ==="
	@for i in $$(seq 1 30); do \
		kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session && break; \
		echo "  Waiting for pod creation... ($$i/30)"; \
		sleep 2; \
	done
	kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s
	kubectl wait --for=condition=Ready pod -l component=taskmanager -n flink-nexmark --timeout=120s 2>/dev/null || true
	@echo "=== Done. Run: make run-local QUERY=q0 ==="

# GKE: Redeploy - update config + restart Flink (or rebuild image if REBUILD=1)
# Usage:
#   make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=<id>              # Update config + restart
#   make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=<id> REBUILD=1    # Rebuild + push + restart
#   make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=<id> REBUILD=1 NO_CACHE=1  # Force full rebuild
redeploy-k8s:
ifndef CONFIG
	@echo "Error: CONFIG not specified"
	@echo "Usage: make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=<id> [REBUILD=1] [NO_CACHE=1]"
	@exit 1
endif
ifndef PROJECT_ID
	@echo "Error: PROJECT_ID not specified"
	@echo "Usage: make redeploy-k8s CONFIG=hudi-gcs PROJECT_ID=your-project [REBUILD=1] [NO_CACHE=1]"
	@exit 1
endif
ifdef REBUILD
	@$(MAKE) push CONFIG=$(CONFIG) PROJECT_ID=$(PROJECT_ID) $(if $(NO_CACHE),NO_CACHE=1,)
endif
	@echo "=== Updating ConfigMap ==="
	kubectl create configmap nexmark-config \
		--from-file=nexmark.yaml="configs/$(NEXMARK_CONFIG_FILE)" \
		-n flink-nexmark --dry-run=client -o yaml | kubectl apply -f -
	@echo "=== Applying metrics service ==="
	kubectl apply -f k8s/nexmark-metrics-service.yaml
	@echo "=== Restarting FlinkDeployment ==="
	kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
	sleep 2
	@IMAGE_URL="$(REGION)-docker.pkg.dev/$(PROJECT_ID)/flink/$(IMAGE_NAME):$(IMAGE_TAG)"; \
	sed "s|image: nexmark-runner:.*|image: $$IMAGE_URL|g" k8s/flink-session.yaml | kubectl apply -f -
	@echo "=== Waiting for Flink pods ==="
	@for i in $$(seq 1 30); do \
		kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session && break; \
		echo "  Waiting for pod creation... ($$i/30)"; \
		sleep 2; \
	done
	kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s
	kubectl wait --for=condition=Ready pod -l component=taskmanager -n flink-nexmark --timeout=120s 2>/dev/null || true
	@echo "=== Done. Run: make run-k8s QUERY=q0 ==="

# GKE: Restart Flink pods (use after Ctrl+C or stuck run)
# Usage: make restart-k8s CONFIG=hudi-gcs PROJECT_ID=<id>
restart-k8s:
ifndef CONFIG
	@echo "Error: CONFIG not specified"
	@echo "Usage: make restart-k8s CONFIG=hudi-gcs PROJECT_ID=your-project"
	@exit 1
endif
ifndef PROJECT_ID
	@echo "Error: PROJECT_ID not specified"
	@echo "Usage: make restart-k8s CONFIG=hudi-gcs PROJECT_ID=your-project"
	@exit 1
endif
	@echo "=== Applying metrics service ==="
	kubectl apply -f k8s/nexmark-metrics-service.yaml
	@echo "=== Restarting FlinkDeployment ==="
	kubectl delete flinkdeployment flink-session -n flink-nexmark --ignore-not-found
	sleep 2
	@IMAGE_URL="$(REGION)-docker.pkg.dev/$(PROJECT_ID)/flink/$(IMAGE_NAME):$(IMAGE_TAG)"; \
	sed "s|image: nexmark-runner:.*|image: $$IMAGE_URL|g" k8s/flink-session.yaml | kubectl apply -f -
	@echo "=== Waiting for Flink pods ==="
	@for i in $$(seq 1 30); do \
		kubectl get pod -n flink-nexmark -l component=jobmanager 2>/dev/null | grep -q flink-session && break; \
		echo "  Waiting for pod creation... ($$i/30)"; \
		sleep 2; \
	done
	kubectl wait --for=condition=Ready pod -l component=jobmanager -n flink-nexmark --timeout=300s
	@echo "=== Done. Run: make run-k8s QUERY=q0 ==="
