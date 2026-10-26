"""The Terraform, graded on the things a reader would get wrong rather than on syntax.

`terraform validate` is the syntax check and it passed against hashicorp google
6.50.0, resolved from the `~> 6.12` constraint. It is not run here, because
the suite has to pass on a machine with no terraform binary and no network, and
because validate would not catch any of the four below. Each one is a configuration
that validates cleanly and is wrong.

Nothing in infra/ has been applied. These checks grade the configuration as text.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infra"


def _read(name: str) -> str:
    return (INFRA / name).read_text()


def check_the_identity_pool_restricts_which_repository_may_assume_it():
    """The documented footgun, and the only one here that is a security hole.

    Without `attribute_condition`, the pool trusts a GitHub OIDC token from ANY
    repository on GitHub that names the provider. `attribute_mapping` reads as the
    restriction and is not one. It only makes the claim available to the condition.
    """
    oidc = _read("oidc.tf")
    if "attribute_condition" not in oidc:
        raise AssertionError(
            "oidc.tf declares a workload identity pool provider with no "
            "attribute_condition, so any GitHub repository could assume the CI "
            "service account"
        )
    if "assertion.repository ==" not in oidc:
        raise AssertionError(
            "the attribute_condition in oidc.tf does not pin assertion.repository, so "
            "it is not restricting by repository"
        )


def check_the_release_bucket_refuses_a_terraform_delete_while_it_holds_objects():
    main = _read("main.tf")
    if "force_destroy = false" not in main:
        raise AssertionError(
            "the release bucket does not set force_destroy = false, so a "
            "`terraform destroy` would empty a published release while the pointer "
            "files in git keep claiming the data is retrievable"
        )


def check_continuous_integration_cannot_delete_a_published_object():
    """objectCreator and objectViewer, never objectAdmin, which carries delete.

    The release is content addressed and append only. CI writes new objects and has
    no reason to remove one, so the grant says so.
    """
    main = _read("main.tf")
    for forbidden in ("roles/storage.objectAdmin", "roles/storage.admin"):
        if forbidden in main:
            raise AssertionError(
                f"main.tf grants {forbidden}, which carries delete on a bucket holding "
                "a published release"
            )
    if "roles/storage.objectCreator" not in main:
        raise AssertionError("main.tf never grants the CI account write on the bucket")


def check_no_project_identifier_is_committed():
    """A placeholder project id is the thing that survives into a demo and reads real.

    The three identifying variables carry no default on purpose and `terraform.tfvars`
    is gitignored, so the only way one of them acquires a value is somebody filling it
    in, which is the moment the cloud side stops being hypothetical.
    """
    for name in ("project_id", "bucket_name", "billing_account"):
        block = _read("variables.tf").split(f'variable "{name}"', 1)
        if len(block) != 2:
            raise AssertionError(f"variables.tf declares no variable named {name}")
        # Comments are stripped before the match. The first version of this check
        # looked for the word anywhere in the block and fired on a comment reading
        # "cannot have a useful default", which is the opposite of the thing it is
        # trying to catch.
        body = block[1].split("}", 1)[0]
        assignments = [
            line for line in body.splitlines() if not line.strip().startswith("#")
        ]
        if any(line.split("=")[0].strip() == "default" for line in assignments if "=" in line):
            raise AssertionError(
                f"variable {name} carries a default, so `terraform plan` would run "
                "against a plausible and wrong project"
            )
    committed = sorted(p.name for p in INFRA.glob("*.tfvars"))
    if committed:
        raise AssertionError(f"infra/ holds {committed}, which should never be committed")


def check_a_budget_alert_exists_and_watches_for_an_overrun_rather_than_a_drift():
    """A budget goes in before anything billable exists, not after a surprise.

    This project ran local first so the budget is late, and it is here rather than
    absent. The
    threshold above 100 percent is the one that matters on a 265 MB bucket, because
    the realistic failure is egress from something reading it in a loop and not a
    slow climb in storage.
    """
    budget = _read("budget.tf")
    if "google_billing_budget" not in budget:
        raise AssertionError("infra/ declares no budget, and no cycle 3 project ships without one")
    if "2.0" not in budget:
        raise AssertionError(
            "the budget declares no threshold above 100 percent, so a sudden overrun "
            "looks the same as reaching the budget"
        )
