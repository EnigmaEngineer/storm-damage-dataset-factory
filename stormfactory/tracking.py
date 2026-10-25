"""Experiment tracking and retrieval tracing, neither of which runs here yet.

Two external services belong in this picture and neither is reachable from this
repository today.

Weights and Biases would track the damage baseline runs and hold each release as an
artifact the run consumed, so a reported figure points back at the exact dataset
version behind it. Right now the baseline writes its own manifest and the release
carries its own digests, which covers the same ground for a reader with the repo in
front of them. What it does not give is a run history across versions.

Langfuse would trace a retrieval assistant over the narratives, making per query
cost and latency visible. The vector column and its filtered search exist. The
assistant over them does not.

Both are blocked on the same thing, and it is a credential rather than a design
question. There is no cloud identity for this repository to assume. The deployment
route is short lived credentials issued to continuous integration through an OIDC
trust, and that trust has not been created, so there is no non secret project
identifier to put in a configuration file and nothing to authenticate as.

The functions below raise. That is the point. A tracking module that quietly does
nothing when it is unconfigured produces a run that looks tracked and is not, and
the absence shows up much later as a gap in a history nobody can rebuild. So an
unconfigured call is an error with the reason in it, and the caller decides.

TODO: wire both services once the OIDC trust for the project exists and the
non secret identifiers are recorded. Needed before either can run:
  - the cloud project identifier and the workload identity federation provider
    resource name, neither of which is recorded anywhere in this repository yet
  - a continuous integration service account with permission to mint a short lived
    token for the tracking services
  - the Weights and Biases entity and project names, and the Langfuse host
Until all three exist this module stays unconfigured and every entry point here
raises rather than returning a no-op.
"""

from __future__ import annotations

import os

# The environment variables each service would read. Named here so a check can
# assert the module reports itself unconfigured, rather than a test hard coding
# strings that drift away from what the code looks at.
WANDB_VARIABLES = ("WANDB_ENTITY", "WANDB_PROJECT", "WANDB_API_KEY")
LANGFUSE_VARIABLES = ("LANGFUSE_HOST", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")

BLOCKER = (
    "no cloud identity exists for this repository. The deployment route issues short "
    "lived credentials to continuous integration through an OIDC trust, and that trust "
    "has not been created, so there is no project identifier to configure and nothing "
    "to authenticate as"
)


class NotConfigured(RuntimeError):
    """Raised when something asks for tracking that cannot happen yet."""


def missing(variables) -> list[str]:
    return [name for name in variables if not os.environ.get(name)]


def wandb_status() -> dict:
    absent = missing(WANDB_VARIABLES)
    return {
        "service": "weights and biases",
        "configured": not absent,
        "missing_variables": absent,
        "intended_use": "track the damage baseline runs and hold each release as a consumed artifact",
        "blocker": BLOCKER,
    }


def langfuse_status() -> dict:
    absent = missing(LANGFUSE_VARIABLES)
    return {
        "service": "langfuse",
        "configured": not absent,
        "missing_variables": absent,
        "intended_use": "trace a retrieval assistant over the narratives, per query cost and latency",
        "blocker": BLOCKER,
    }


def status() -> dict:
    return {"wandb": wandb_status(), "langfuse": langfuse_status()}


def log_baseline_run(metrics: dict, dataset_version: str):
    """Record a baseline run against a dataset version.

    TODO: implement against the tracking API once the OIDC trust exists. The call
    would open a run, attach the release version as the consumed artifact, and log
    the arm figures the baseline already computes.
    """
    state = wandb_status()
    if not state["configured"]:
        raise NotConfigured(
            "cannot log a baseline run, missing "
            f"{', '.join(state['missing_variables'])}. {BLOCKER}"
        )
    raise NotConfigured(
        "the credentials are present and the client call is still unwritten, which is "
        "a second thing to do rather than a reason to report a run as logged"
    )


def trace_retrieval(query: str, returned: list, cost_tokens: int | None = None):
    """Trace one retrieval against the vector column.

    TODO: implement against the tracing API once the host and keys exist. There is
    also no retrieval assistant over the narratives yet, so this has no caller.
    """
    state = langfuse_status()
    if not state["configured"]:
        raise NotConfigured(
            "cannot trace a retrieval, missing "
            f"{', '.join(state['missing_variables'])}. {BLOCKER}"
        )
    raise NotConfigured(
        "the credentials are present and the client call is still unwritten, which is "
        "a second thing to do rather than a reason to report a trace as sent"
    )
