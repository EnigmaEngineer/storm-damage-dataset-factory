# The cloud side of the release, which has not been applied

This directory holds the Terraform for the place a published release would live. A
bucket for the DVC remote, a service account for continuous integration to act as,
and a workload identity federation pool so that account can be assumed from a GitHub
Actions run without a key file existing anywhere.

**None of it has been applied.** `terraform plan` has not run against a project either,
because there is no project. What is here is configuration written against the
provider's documented schema and checked by `terraform validate` and nothing further.
A plan needs credentials and an applied state needs billing, and the repository has
neither. Saying so here rather than letting a reader assume a green directory means
something is the whole reason this file is the first thing in it.

## What is missing, exactly

Three values, none of them secret, and all three are blocked on the same thing.

- the project id
- the workload identity federation provider resource name, which is the long
  `projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`
  string a workflow names when it exchanges its token
- the service account email continuous integration acts as

They are blocked because the OIDC trust between the cloud project and this GitHub
repository does not exist. Creating it is a one time console or `gcloud` step by
somebody holding owner on a billing enabled project, and a scheduled build has no
business doing it even if it could. The standing rule for this portfolio is that a
build never handles a credential, so the trust is created out of band and only the
resulting non secret identifiers come back here.

`terraform.tfvars` is therefore absent rather than filled with placeholders. A
placeholder project id is the kind of thing that survives into a demo and gets read
as real.

## What applying it would cost

Storage is the whole bill. The release is 265 MB and most of that is one artifact.

| artifact | bytes | share |
| --- | --- | --- |
| narrative_vectors | 233,385,086 | 88.2% |
| storm_events, five partitions | 26,861,858 | 10.2% |
| event_counties | 1,788,259 | 0.7% |
| event_splits | 1,476,562 | 0.6% |
| spc_reports | 939,203 | 0.4% |
| fema_declarations | 74,949 | 0.0% |
| imagery_tiles | 12,735 | 0.0% |
| total | 264,538,652 | 100% |

Those are the byte counts in `measurements/release.json`, re-derived by
`tests/test_readme.py` rather than typed here. At standard regional storage that is
under a dollar a month, and it stays under a dollar until something starts reading it
often. The budget alert in `budget.tf` is set low for that reason. It is there to
catch a mistake, not to track a bill.

## Teardown

`scripts/teardown.sh` is the one command. It is written and it has not been run,
because there is nothing to destroy. Read it before trusting it.
