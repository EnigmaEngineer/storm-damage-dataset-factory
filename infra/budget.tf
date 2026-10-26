# A budget alert goes in before anything billable exists, not after a surprise. This
# one is late, because the project ran local first and had nothing to bill until now.

resource "google_billing_budget" "release_storage" {
  billing_account = var.billing_account
  display_name    = "storm-damage-dataset-factory"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_amount_usd)
    }
  }

  # Half, all, and then double. The third threshold is the one that matters. A bill
  # at twice the budget on a bucket holding 265 MB means something is reading it in a
  # loop, and that is a different problem from a slow drift upward.
  dynamic "threshold_rules" {
    for_each = [0.5, 1.0, 2.0]
    content {
      threshold_percent = threshold_rules.value
    }
  }
}
