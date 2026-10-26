# The trust that makes a key file unnecessary. A GitHub Actions run presents its own
# OIDC token, this pool validates it, and the run gets a short lived credential for
# the service account. No long lived key exists to leak.

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  # Without this, the pool trusts a token from ANY repository on GitHub that names
  # this provider. The attribute mapping above is not a restriction, it only makes
  # the claim available. This condition is the restriction and leaving it out is the
  # documented way people get this wrong.
  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# The member is a principal set rather than a principal. It names every identity the
# pool will mint for this repository, which is one per workflow run, so there is no
# single account to grant and the grant is over the set.
resource "google_service_account_iam_member" "github_may_impersonate_ci" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}
