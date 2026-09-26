"""Concurrent commit semantics.

* N writers committing from the same base: exactly one wins, all others
  are rejected with ``concurrent_commit_conflict`` -- no partial writes.
* Every failed commit leaves table contents untouched.
* A rejected commit can be retried against the new base and succeeds.
"""

from __future__ import annotations

import logging
import threading

import pytest

from lakehouse_catalog.errors import (
    ConcurrentCommitConflictError,
    ValidationError,
)

log = logging.getLogger("lakehouse_catalog.test")


def _append(catalog, commit_id: str, msg: str, base=1) -> dict:
    req = {"append": [{"rows": [{"id": 1, "msg": msg, "dt": "d1"}],
                       "partition": {"dt": "d1"}}]}
    if base is not None:
        req["base_version"] = base
    return catalog.commit("events", req, commit_id=commit_id)


def test_parallel_commits_same_base_exactly_one_wins(simple_table):
    catalog = simple_table
    n = 8
    results: list[object] = []
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        barrier.wait()  # maximize contention
        try:
            out = _append(catalog, f"c-{i}", f"m{i}")
            results.append(("ok", out["version"]))
        except ConcurrentCommitConflictError as exc:
            results.append(("conflict", exc.details["reason"]))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    oks = [r for r in results if r[0] == "ok"]
    conflicts = [r for r in results if r[0] == "conflict"]
    log.info("parallel commit decision: winners=%s losers=%d", oks, len(conflicts))
    assert len(oks) == 1, results
    assert len(conflicts) == n - 1
    assert all(
        r[1] == "base_version_not_current" for r in conflicts
    ), conflicts

    # Atomicity: only one complete new data file, row count exactly one.
    read = catalog.read_table("events")
    assert read["version"] == 2
    assert read["row_count"] == 1
    assert len(read["files"]) == 1


def test_conflict_details_name_base_and_current(simple_table):
    catalog = simple_table
    _append(catalog, "first", "x", base=1)
    with pytest.raises(ConcurrentCommitConflictError) as ei:
        _append(catalog, "late", "y", base=1)
    details = ei.value.details
    log.info("conflict basis: commit_id=%s details=%s", "late", details)
    assert details["base_version"] == 1
    assert details["current_version"] == 2
    assert details["reason"] == "base_version_not_current"


def test_commit_on_ahead_of_current_base_rejected(simple_table):
    catalog = simple_table
    with pytest.raises(ValidationError) as ei:
        _append(catalog, "future", "z", base=42)
    assert ei.value.details["reason"] == "base_version_ahead_of_current"
    # no snapshot was created
    assert catalog.list_snapshots("events")["current_version"] == 1


def test_rejected_commit_leaves_no_partial_state(simple_table):
    catalog = simple_table
    before = catalog.list_snapshots("events")
    _append(catalog, "a", "a", base=1)
    with pytest.raises(ConcurrentCommitConflictError):
        _append(catalog, "b", "b", base=1)
    # first wins, second fails; no dangling files for the failed one
    after = catalog.read_table("events")
    assert after["row_count"] == 1
    assert after["version"] == before["current_version"] + 1


def test_failed_commit_can_retry_on_new_base(simple_table):
    catalog = simple_table
    _append(catalog, "first", "x", base=1)
    with pytest.raises(ConcurrentCommitConflictError):
        _append(catalog, "second", "y", base=1)
    # retry reads current state, base retry on version 2
    out = _append(catalog, "second-retry", "y", base=2)
    assert out["version"] == 3
    assert catalog.read_table("events")["row_count"] == 2
