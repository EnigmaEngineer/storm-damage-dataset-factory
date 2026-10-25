"""Checks that the unconfigured tracking path stays loud.

There is no cloud identity for this repository yet, so neither tracking service can
run. The risk that needs a guard is not that the calls fail. It is that somebody
later makes them succeed quietly, by returning None when the credentials are absent,
and a run then looks tracked while nothing was recorded.

So these checks pin the behaviour rather than the implementation. An unconfigured
call raises, the error names what is missing, and the module says what it is waiting
on.
"""

from pathlib import Path

from stormfactory import tracking

REPO_ROOT = Path(__file__).resolve().parent.parent


def check_nothing_is_configured_in_a_plain_environment():
    """Both services report themselves unconfigured with the variables named."""
    state = tracking.status()
    for service in ("wandb", "langfuse"):
        assert not state[service]["configured"], f"{service} reports itself configured"
        assert state[service]["missing_variables"], f"{service} names nothing missing"


def check_logging_a_baseline_run_raises_rather_than_doing_nothing():
    try:
        tracking.log_baseline_run({"precision": 0.5}, "v0.3")
    except tracking.NotConfigured as exc:
        assert "WANDB" in str(exc), str(exc)
        return
    raise AssertionError("an unconfigured baseline log returned instead of raising")


def check_tracing_a_retrieval_raises_rather_than_doing_nothing():
    try:
        tracking.trace_retrieval("a query", [])
    except tracking.NotConfigured as exc:
        assert "LANGFUSE" in str(exc), str(exc)
        return
    raise AssertionError("an unconfigured trace returned instead of raising")


def check_neither_entry_point_returns_a_value_on_any_path():
    """Every path out of both entry points raises.

    A credentialed environment would reach the second raise rather than returning,
    because the client call is still unwritten. Reporting a run as logged when the
    call does not exist is the failure this guards.
    """
    source = (REPO_ROOT / "stormfactory" / "tracking.py").read_text()
    for name in ("def log_baseline_run", "def trace_retrieval"):
        body = source.split(name, 1)[1].split("\ndef ", 1)[0]
        # Statements, not substrings. One of these takes a parameter called
        # `returned`, and a substring test on the whole body reads that as a return.
        statements = [line.strip() for line in body.splitlines()]
        offenders = [line for line in statements if line == "return" or line.startswith("return ")]
        assert not offenders, f"{name} has a return path and every path should raise: {offenders}"


def check_the_blocker_is_named_and_is_not_a_vague_one():
    assert "OIDC" in tracking.BLOCKER, tracking.BLOCKER
    for service in tracking.status().values():
        assert service["blocker"] == tracking.BLOCKER
        assert service["intended_use"], "a blocked service does not say what it was for"


def check_the_module_carries_a_todo_naming_what_it_waits_on():
    """A placeholder without a specific blocker is indistinguishable from an omission."""
    source = (REPO_ROOT / "stormfactory" / "tracking.py").read_text()
    assert "TODO" in source, "the blocked module carries no TODO"
    todo = source.split("TODO", 1)[1]
    for expected in ("OIDC", "identifier"):
        assert expected in todo, f"the TODO does not name {expected}"


def check_the_variable_lists_are_what_the_code_reads():
    """The names in the lists are the names the status functions look at."""
    import os

    saved = {name: os.environ.get(name) for name in tracking.WANDB_VARIABLES}
    try:
        for name in tracking.WANDB_VARIABLES:
            os.environ[name] = "set"
        assert tracking.wandb_status()["configured"], (
            "every declared variable was set and the module still reports unconfigured"
        )
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
