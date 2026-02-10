# Create Flink namespace
resource "kubernetes_namespace" "flink" {
  metadata {
    name = var.flink_namespace
  }

  depends_on = [google_container_node_pool.jobmanager_nodes, google_container_node_pool.taskmanager_nodes]
}

# cert-manager namespace
resource "kubernetes_namespace" "cert_manager" {
  metadata {
    name = "cert-manager"
  }

  depends_on = [google_container_node_pool.jobmanager_nodes, google_container_node_pool.taskmanager_nodes]
}

# Install cert-manager (required for Flink Operator webhooks)
resource "helm_release" "cert_manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  version    = var.cert_manager_version
  namespace  = kubernetes_namespace.cert_manager.metadata[0].name

  set {
    name  = "installCRDs"
    value = "true"
  }

  # Wait for cert-manager to be fully ready
  wait    = true
  timeout = 600
}

# Install Flink Kubernetes Operator
resource "helm_release" "flink_operator" {
  name       = "flink-kubernetes-operator"
  repository = "https://downloads.apache.org/flink/flink-kubernetes-operator-${var.flink_operator_version}/"
  chart      = "flink-kubernetes-operator"
  version    = var.flink_operator_version
  namespace  = kubernetes_namespace.flink.metadata[0].name

  # Webhook configuration
  set {
    name  = "webhook.create"
    value = "true"
  }

  # Watch the Flink namespace
  set {
    name  = "watchNamespaces[0]"
    value = var.flink_namespace
  }

  # Default Flink configuration for all deployments
  values = [
    yamlencode({
      defaultConfiguration = {
        create = true
        append = true
        "flink-conf.yaml" = <<-EOT
          # Checkpointing
          execution.checkpointing.interval: 60000
          execution.checkpointing.mode: EXACTLY_ONCE
          state.backend: rocksdb
          state.checkpoints.dir: gs://${data.google_storage_bucket.hudi_tables.name}/checkpoints
          state.savepoints.dir: gs://${data.google_storage_bucket.hudi_tables.name}/savepoints

          # GCS authentication via Workload Identity
          gs.auth.type: APPLICATION_DEFAULT

          # Memory configuration defaults
          taskmanager.memory.process.size: 8192m
          jobmanager.memory.process.size: 2048m
          taskmanager.numberOfTaskSlots: 4

          # High availability (optional, disabled for research)
          # high-availability.type: kubernetes
          # high-availability.storageDir: gs://${data.google_storage_bucket.hudi_tables.name}/ha
        EOT
      }
    })
  ]

  depends_on = [helm_release.cert_manager]

  wait    = true
  timeout = 600
}

# Annotate the Flink job service account for Workload Identity
# (The 'flink' SA is created by Helm; we add the GCP SA annotation for GCS access)
resource "kubernetes_annotations" "flink_sa_workload_identity" {
  api_version = "v1"
  kind        = "ServiceAccount"

  metadata {
    name      = "flink"
    namespace = var.flink_namespace
  }

  annotations = {
    "iam.gke.io/gcp-service-account" = google_service_account.flink_sa.email
  }

  depends_on = [helm_release.flink_operator]
}

# Note: RBAC for Flink jobs is managed by the Helm chart (creates 'flink' role
# and 'flink-role-binding'). Do not create separate RBAC resources here as they
# conflict with Helm-managed ones.
