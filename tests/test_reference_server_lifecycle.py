"""Lifecycle guards for tests/reference_server.sh (Docker mode).

No Docker is involved: a shim named ``docker`` is put first on PATH and records every
invocation, and the two probes that need a live server (``_compat_ref_wait_ping``,
``_compat_ref_validate_pin``) are redefined after sourcing. What is asserted is the
part the benchmark harness (docs/plans/rts-comparative-benchmarks-plan.md) depends on
before it can trust the shared helper:

* the compose file / project overrides are applied, and defaults are unchanged;
* a failed ``up`` still leaves the container *owned*, so the caller's teardown stops it;
* an already-running container is never claimed;
* a caller's ``trap ... INT TERM`` reaches ``compat_reference_stop`` on a signal.
"""

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
HELPER = REPO_ROOT / "tests" / "reference_server.sh"
DEFAULT_COMPOSE = REPO_ROOT / "docker-compose.compat.yml"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash is required")

# Every `docker` call lands here as one line: "<args joined by \t>".
DOCKER_SHIM = r"""#!/usr/bin/env bash
printf '%s\n' "$(IFS=$'\t'; echo "$*")" >> "$DOCKER_SHIM_LOG"
# `docker compose ... ps -q reference`: print an id when told a container already exists.
if [[ " $* " == *" ps -q reference "* ]]; then
    if [[ -n "${DOCKER_SHIM_ALREADY:-}" ]]; then
        echo "deadbeefcafe"
    fi
    exit 0
fi
if [[ " $* " == *" up "* ]]; then
    exit "${DOCKER_SHIM_UP_STATUS:-0}"
fi
exit 0
"""

# Sourced after the helper: replace the live-server probes with no-ops.
STUBS = """
_compat_ref_wait_ping() { return 0; }
_compat_ref_validate_pin() { return 0; }
"""


@pytest.fixture
def shim(tmp_path):
    """A PATH entry whose `docker` is the recording shim, plus its log file."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(DOCKER_SHIM)
    docker.chmod(0o755)
    log = tmp_path / "docker.log"
    log.touch()
    return bin_dir, log


def _run(shim, script, env_extra=None, timeout=30):
    bin_dir, log = shim
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["DOCKER_SHIM_LOG"] = str(log)
    # Never fall through to a real reference: the helper must pick Docker mode.
    env["COMPAT_REFERENCE_MODE"] = "docker"
    env.pop("COMPAT_REFERENCE_URL", None)
    env.pop("COMPAT_KEEP_REFERENCE", None)
    env.pop("COMPAT_REFERENCE_COMPOSE_FILE", None)
    env.pop("COMPAT_REFERENCE_COMPOSE_PROJECT", None)
    if env_extra:
        env.update(env_extra)
    full = f". '{HELPER}'\n{STUBS}\n{script}"
    return subprocess.run(
        ["bash", "-c", full], env=env, capture_output=True, text=True, timeout=timeout
    )


def _calls(log):
    return [line.split("\t") for line in log.read_text().splitlines() if line]


def _find(calls, verb):
    """The single docker call whose compose verb is `verb` (up/stop/ps)."""
    matches = [c for c in calls if verb in c and c[0] == "compose"]
    assert len(matches) == 1, f"expected one `{verb}` call, got {matches}"
    return matches[0]


def _compose_selection(call):
    """(-f value, -p value or None) from a recorded `docker compose` argv."""
    file = call[call.index("-f") + 1]
    project = call[call.index("-p") + 1] if "-p" in call else None
    return file, project


START_AND_REPORT = """
compat_reference_start; rc=$?
echo "rc=$rc owned=$COMPAT_REFERENCE_OWNED kind=$COMPAT_REFERENCE_KIND url=$COMPAT_REFERENCE_URL"
compat_reference_stop
"""


def test_defaults_are_the_compat_file_under_the_default_project(shim):
    result = _run(shim, START_AND_REPORT)
    assert result.returncode == 0, result.stderr
    assert "rc=0 owned=1 kind=docker url=redis://127.0.0.1:16379" in result.stdout

    calls = _calls(shim[1])
    up = _find(calls, "up")
    assert _compose_selection(up) == (str(DEFAULT_COMPOSE), None)
    assert up[-1] == "reference" and "--wait" in up
    stop = _find(calls, "stop")
    assert _compose_selection(stop) == (str(DEFAULT_COMPOSE), None)


def test_compose_file_and_project_overrides_apply_to_start_and_stop(shim, tmp_path):
    overlay = tmp_path / "docker-compose.custom.yml"
    overlay.write_text("services:\n  reference:\n    image: scratch\n")
    result = _run(
        shim,
        START_AND_REPORT,
        env_extra={
            "COMPAT_REFERENCE_COMPOSE_FILE": str(overlay),
            "COMPAT_REFERENCE_COMPOSE_PROJECT": "bench-under-test",
            "COMPAT_REFERENCE_PORT": "16479",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "rc=0 owned=1 kind=docker url=redis://127.0.0.1:16479" in result.stdout

    calls = _calls(shim[1])
    for verb in ("ps", "up", "stop"):
        assert _compose_selection(_find(calls, verb)) == (str(overlay), "bench-under-test")


def test_missing_compose_file_fails_before_touching_docker(shim, tmp_path):
    result = _run(
        shim,
        START_AND_REPORT,
        env_extra={"COMPAT_REFERENCE_COMPOSE_FILE": str(tmp_path / "nope.yml")},
    )
    assert "rc=1 owned=0" in result.stdout
    assert "compose file not found" in result.stderr
    assert _calls(shim[1]) == []


def test_failed_up_is_still_owned_and_stopped(shim):
    """A container that was created but never became healthy must not leak."""
    result = _run(shim, START_AND_REPORT, env_extra={"DOCKER_SHIM_UP_STATUS": "1"})
    assert "rc=1 owned=1 kind=docker" in result.stdout
    assert "could not start the reference container" in result.stderr

    calls = _calls(shim[1])
    _find(calls, "up")
    assert _compose_selection(_find(calls, "stop")) == (str(DEFAULT_COMPOSE), None)


def test_already_running_container_is_never_claimed(shim):
    result = _run(shim, START_AND_REPORT, env_extra={"DOCKER_SHIM_ALREADY": "1"})
    assert result.returncode == 0, result.stderr
    assert "rc=0 owned=0 kind=docker" in result.stdout
    assert "already running" in result.stderr

    calls = _calls(shim[1])
    _find(calls, "up")
    assert not [c for c in calls if "stop" in c], "must not stop a container we did not start"


def test_failed_up_of_an_already_running_container_is_not_stopped(shim):
    result = _run(
        shim,
        START_AND_REPORT,
        env_extra={"DOCKER_SHIM_ALREADY": "1", "DOCKER_SHIM_UP_STATUS": "1"},
    )
    assert "rc=1 owned=0 kind=docker" in result.stdout
    assert not [c for c in _calls(shim[1]) if "stop" in c]


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_signal_reaches_the_callers_teardown(shim, tmp_path, sig):
    """Mirror the build.sh/fuzz.sh pattern: the caller installs the trap, the helper
    only exposes stop. A signal mid-run must still stop the owned container."""
    ready = tmp_path / "ready"
    script = f"""
cleanup() {{ local status=$?; set +e; compat_reference_stop; exit "$status"; }}
trap cleanup EXIT INT TERM
compat_reference_start || exit 1
touch '{ready}'
while :; do sleep 0.1; done
"""
    bin_dir, log = shim
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["DOCKER_SHIM_LOG"] = str(log)
    env["COMPAT_REFERENCE_MODE"] = "docker"
    env["COMPAT_REFERENCE_COMPOSE_PROJECT"] = "signal-under-test"
    env.pop("COMPAT_REFERENCE_URL", None)
    env.pop("COMPAT_KEEP_REFERENCE", None)
    env.pop("COMPAT_REFERENCE_COMPOSE_FILE", None)
    proc = subprocess.Popen(
        ["bash", "-c", f". '{HELPER}'\n{STUBS}\n{script}"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 20
        while not ready.exists():
            if proc.poll() is not None:
                pytest.fail(f"script exited early: {proc.stderr.read()}")
            if time.monotonic() > deadline:
                pytest.fail("script never reached the ready marker")
            time.sleep(0.05)
        proc.send_signal(sig)
        proc.wait(timeout=20)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    calls = _calls(log)
    stop = _find(calls, "stop")
    assert _compose_selection(stop) == (str(DEFAULT_COMPOSE), "signal-under-test")
