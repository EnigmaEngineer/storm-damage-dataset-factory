# No defaults on the three that identify a real project. A default here would let
# `terraform plan` run against something plausible and wrong, and the failure mode is
# a resource created in whatever project the ambient credentials happen to point at.
#
# TODO: fill these in once an OIDC trust exists between this repository and a billing
# enabled GCP project. Creating that trust is a one time step by somebody holding owner
# on the project, and it is deliberately not something an automated build does. Until
# it exists `terraform plan` cannot run and nothing here has been applied.

variable "project_id" {
  description = "The GCP project that holds the bucket and the CI identity."
  type        = string
}

variable "github_repository" {
  description = "owner/name of the repository allowed to assume the CI identity."
  type        = string
  default     = "EnigmaEngineer/storm-damage-dataset-factory"
}

variable "region" {
  description = "Regional location for the release bucket."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  # Buckets share one global namespace, so this cannot have a useful default. The
  # project id is the usual prefix people reach for and it is also a variable here.
  description = "Globally unique name for the DVC remote bucket."
  type        = string
}

variable "budget_amount_usd" {
  # The release is 265 MB and regional storage on that is cents a month. Ten dollars
  # is not a forecast, it is the line past which something has gone wrong, most
  # likely egress from a reader pulling the vector column in a loop.
  description = "Monthly budget in USD. An alert fires at the thresholds below."
  type        = number
  default     = 10
}

variable "billing_account" {
  description = "Billing account id the budget attaches to."
  type        = string
}
