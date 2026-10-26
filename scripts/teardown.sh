#!/usr/bin/env bash
# Destroy everything infra/ creates, and say what it cannot reach.
#
# NOT RUN. There is no project and nothing has been applied, so this has never
# destroyed anything. It is here because the rule for this portfolio is that a
# project does not finish with billable resources running, and a teardown written
# on the last day against resources that exist is a teardown written in a hurry.
#
# Read it before trusting it. In particular read the two things it refuses to do.
set -euo pipefail

cd "$(dirname "$0")/../infra"

if [ ! -f terraform.tfvars ]; then
    cat >&2 <<'MSG'
terraform.tfvars is absent, so there is nothing applied for this to destroy.

That is the expected state today. The OIDC trust for this repository does not exist,
so no project id was ever filled in and no apply ever ran. See infra/README.md.
MSG
    exit 2
fi

echo "resources terraform believes it owns:"
terraform state list

# The bucket carries force_destroy = false on purpose, so this fails rather than
# deleting a release. Emptying it is a separate deliberate act by a person who has
# decided the release is finished, and it is not something a teardown script should
# do on their behalf.
cat <<'MSG'

The release bucket will NOT be destroyed by the command below while it holds objects.
force_destroy is false in main.tf. If the release is genuinely finished:

    gcloud storage rm --recursive gs://<bucket>/dvc

then re-run this. Until then terraform will stop on the bucket and leave the rest.
MSG

terraform destroy -auto-approve

# The budget outlives the project it watches, because it is attached to the billing
# account rather than to the project, and a destroyed project with a live budget is a
# thing nobody looks at again.
echo
echo "check the billing account for a leftover budget named storm-damage-dataset-factory"
echo "terraform owns it, so the destroy above should have taken it. Verify rather than assume:"
echo "  gcloud billing budgets list --billing-account <id>"
