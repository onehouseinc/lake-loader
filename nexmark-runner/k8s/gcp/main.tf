# Provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# VPC Network
resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
  project                 = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "${var.cluster_name}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
  project       = var.project_id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/20"
  }
}

# GKE Cluster
resource "google_container_cluster" "flink_cluster" {
  name     = var.cluster_name
  location = var.zone
  project  = var.project_id

  # Use separately managed node pool
  remove_default_node_pool = true
  initial_node_count       = 1

  # Network configuration
  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # Workload Identity for secure GCS access
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Release channel for automatic upgrades
  release_channel {
    channel = "REGULAR"
  }

  depends_on = [google_project_service.required_apis]
}

# Node pool for Flink JobManager (fixed size, no autoscaling)
resource "google_container_node_pool" "jobmanager_nodes" {
  name     = "jobmanager-pool"
  cluster  = google_container_cluster.flink_cluster.name
  location = var.zone
  project  = var.project_id

  node_count = var.jm_node_count

  node_config {
    machine_type = var.jm_machine_type
    disk_size_gb = var.jm_disk_size_gb
    disk_type    = "pd-ssd"

    # Workload Identity
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      "flink-role" = "jobmanager"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Node pool for Flink TaskManagers (fixed size, no autoscaling)
resource "google_container_node_pool" "taskmanager_nodes" {
  name     = "taskmanager-pool"
  cluster  = google_container_cluster.flink_cluster.name
  location = var.zone
  project  = var.project_id

  node_count = var.tm_node_count

  node_config {
    machine_type = var.tm_machine_type
    disk_size_gb = var.tm_disk_size_gb
    disk_type    = "pd-ssd"

    # Workload Identity
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      "flink-role" = "taskmanager"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Artifact Registry for Docker images
resource "google_artifact_registry_repository" "flink_repo" {
  location      = var.region
  repository_id = "flink"
  description   = "Docker repository for Flink images"
  format        = "DOCKER"
  project       = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Configure Kubernetes provider
data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.flink_cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.flink_cluster.master_auth[0].cluster_ca_certificate)
}

# Configure Helm provider
provider "helm" {
  kubernetes {
    host                   = "https://${google_container_cluster.flink_cluster.endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.flink_cluster.master_auth[0].cluster_ca_certificate)
  }
}
