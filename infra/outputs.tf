# These three are what `cloud-ids.md` is waiting for, which is why they are outputs
# rather than values a reader has to go and look up in a console after an apply.

output "dvc_remote_url" {
  description = "Set this as the dvc remote url: dvc remote add -d gcs <this>"
  value       = "gs://${google_storage_bucket.release.name}/dvc"
}

output "ci_service_account_email" {
  value = google_service_account.ci.email
}

output "workload_identity_provider" {
  description = "The resource name a GitHub Actions workflow names to exchange its token."
  value       = google_iam_workload_identity_pool_provider.github.name
}
