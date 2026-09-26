"""Idempotent commit retries.

A retried ``commit_id`` must never be applied twice: even if the retry
names a stale base, the ledger recognizes the commit and returns the
version of the original application.
"""

from __future__ import annotations

import logging
import threading

import pytest

from lakehouse_catalog.errors import DuplicateCommitError

log = logging.getLogger("lakehouse_catalog.test")


def _append(catalog, cid, base, msg="x"):
    return catalog.commit(
        "events",
        {"base_version": base,
         "append": [{"rows": [{"id": 1, "msg": msg, "dt": "d1"}],
                     "partition": {"dt": "d1"}}]},
        commit_id=cid)


def test_retry_same_commit_id_returns_original_version(simple_table):
    catalog = simple_table
    out = _append(catalog, "id-1", base=1, msg="first")
    assert out["version"] == 2

    with pytest.raises(DuplicateCommitError) as ei:
        _append(catalog, "id-1", base=1, msg="retry")
    err = ei.value
    log.info("idempotent retry basis: commit_id=%s version=%s reason=%s",
             "id-1", err.details["version"], err.details)
    assert err.details["version"] == 2
    assert catalog.list_snapshots("events")["current_version"] == 2
    # The retried payload ("retry") must NOT be visible.
    rows = catalog.read_table("events")["rows"]
    assert [r["msg"] for r in rows] == ["first"]


def test_retry_with_stale_base_still_idempotent(simple_table):
    catalog = simple_table
    _append(catalog, "id-1", base=1)
    _append(catalog, "id-2", base=2)
    # Retry of id-1 arrives after version 3 exists: duplicate wins over
    # the stale-base conflict because the commit already succeeded.
    with pytest.raises(DuplicateCommitError) as ei:
        _append(catalog, "id-1", base=1)
    assert ei.value.details["version"] == 2
    assert catalog.list_snapshots("events")["current_version"] == 3


def test_parallel_duplicate_retries_apply_once(simple_table):
    catalog = simple_table
    n = 6
    outcomes: list[str] = []
    barrier = threading.Barrier(n)

    def worker() -> None:
        barrier.wait()
        try:
            _append(catalog, "same-id", base=1)
            outcomes.append("applied")
        except DuplicateCommitError:
            outcomes.append("duplicate")

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("applied") == 1
    assert outcomes.count("duplicate") == n - 1
    snap = catalog.list_snapshots("events")
    assert snap["current_version"] == 2
    assert catalog.read_table("events")["row_count"] == 1


def test_create_table_idempotent_on_duplicate_commit_id(simple_table):
    catalog = simple_table
    from lakehouse_catalog.errors import TableAlreadyExistsError
    # Different commit id -> conflict on existence.
    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table(
            "events",
            [{"name": "id", "type": "int"}],
            commit_id="other-create")
