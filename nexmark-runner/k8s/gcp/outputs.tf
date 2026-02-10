output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP Region"
  value       = var.region
}

output "zone" {
  description = "GCP Zone"
  value       = var.zone
}

output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.flink_cluster.name
}

output "cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = google_container_cluster.flink_cluster.endpoint
  sensitive   = true
}

output "gcs_bucket_name" {
  description = "GCS bucket for Hudi tables"
  value       = data.google_storage_bucket.hudi_tables.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL"
  value       = "gs://${data.google_storage_bucket.hudi_tables.name}"
}

output "flink_service_account_email" {
  description = "Flink GCP service account email"
  value       = google_service_account.flink_sa.email
}

output "artifact_registry_url" {
  description = "Artifact Registry URL for Docker images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.flink_repo.repository_id}"
}

output "flink_image_url" {
  description = "Full URL for the Hudi Nexmark image"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.flink_repo.repository_id}/hudi-nexmark:latest"
}

output "kubectl_config_command" {
  description = "Command to configure kubectl"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.flink_cluster.name} --zone ${var.zone} --project ${var.project_id}"
}
