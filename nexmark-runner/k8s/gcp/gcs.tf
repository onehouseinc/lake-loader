# Reference existing GCS bucket for Hudi tables and Flink checkpoints
# Bucket is managed externally (default: hudi-benchmark)
data "google_storage_bucket" "hudi_tables" {
  name = var.gcs_bucket_name
}
