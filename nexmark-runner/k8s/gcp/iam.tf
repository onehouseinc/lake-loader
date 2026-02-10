# Service account for Flink workloads to access GCS
resource "google_service_account" "flink_sa" {
  account_id   = "flink-nexmark-sa"
  display_name = "Flink Nexmark Service Account"
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Grant Storage Object Admin on the existing Hudi bucket
resource "google_storage_bucket_iam_member" "flink_bucket_access" {
  bucket = data.google_storage_bucket.hudi_tables.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.flink_sa.email}"
}

# Workload Identity binding: allow Kubernetes SA to impersonate GCP SA
resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.flink_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.flink_namespace}/flink]"
}

# Grant Artifact Registry Reader for pulling images
resource "google_project_iam_member" "flink_artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.flink_sa.email}"
}
