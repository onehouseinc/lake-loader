variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for GKE cluster"
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  description = "Name of the GKE cluster"
  type        = string
  default     = "flink-nexmark-cluster"
}

# JobManager node pool configuration (fixed size, no autoscaling)
variable "jm_node_count" {
  description = "Number of nodes in the JobManager pool"
  type        = number
  default     = 1
}

variable "jm_machine_type" {
  description = "Machine type for JobManager nodes"
  type        = string
  default     = "e2-standard-8" # 8 vCPU, 32GB RAM
}

# TaskManager node pool configuration (fixed size, no autoscaling)
variable "tm_node_count" {
  description = "Number of nodes in the TaskManager pool"
  type        = number
  default     = 2
}

variable "tm_machine_type" {
  description = "Machine type for TaskManager nodes"
  type        = string
  default     = "e2-standard-8" # 8 vCPU, 32GB RAM
}

variable "jm_disk_size_gb" {
  description = "Disk size in GB for JobManager nodes"
  type        = number
  default     = 20
}

variable "tm_disk_size_gb" {
  description = "Disk size in GB for TaskManager nodes (needs more for checkpoints)"
  type        = number
  default     = 40
}

variable "gcs_bucket_name" {
  description = "Name of the existing GCS bucket for Hudi tables"
  type        = string
  default     = "hudi-benchmark"
}

variable "flink_namespace" {
  description = "Kubernetes namespace for Flink workloads"
  type        = string
  default     = "flink-nexmark"
}

variable "flink_operator_version" {
  description = "Flink Kubernetes Operator Helm chart version"
  type        = string
  default     = "1.10.0"
}

variable "cert_manager_version" {
  description = "cert-manager version"
  type        = string
  default     = "v1.18.2"
}
