terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source = "hashicorp/google"
      # Pinned to a minor rather than floated. A provider upgrade that changes a
      # default on a bucket is a data decision, and it should arrive in a diff
      # somebody reads rather than on the next apply.
      version = "~> 6.12"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "release" {
  name     = var.bucket_name
  location = var.region

  # The DVC cache is content addressed, so an object's name is its hash and an
  # object is never rewritten in place. Versioning would therefore hold nothing but
  # the one version of each object and still be billed for the metadata, which is
  # why it is off here and would not be off on an ordinary data bucket.
  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true

  # A release is an append only history, so nothing here deletes. The rule moves
  # cold objects to a cheaper class instead. 90 days is well past the point where a
  # superseded release stops being pulled and comfortably short of a year.
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Refuse a delete through terraform while objects are in it. A release remote that
  # a `terraform destroy` can empty by accident is worse than no remote, because the
  # pointer files in git keep claiming the data is retrievable.
  force_destroy = false
}

resource "google_service_account" "ci" {
  account_id   = "storm-release-ci"
  display_name = "Pushes release artifacts to the DVC remote from CI"
}

# Write on the bucket and nothing else. Not project level storage admin, and not
# objectAdmin, which carries delete. CI pushes new content addressed objects and has
# no reason to remove one.
resource "google_storage_bucket_iam_member" "ci_writes_objects" {
  bucket = google_storage_bucket.release.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_storage_bucket_iam_member" "ci_reads_objects" {
  bucket = google_storage_bucket.release.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ci.email}"
}

# Public read, because the point of the release is that somebody can pull it without
# asking anybody. This is the one grant here that is deliberately wide, and it is
# wide on object reads only.
resource "google_storage_bucket_iam_member" "anyone_reads_objects" {
  bucket = google_storage_bucket.release.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}
