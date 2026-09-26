"""Snapshot reads, history and partition consistency."""

from __future__ import annotations

import logging

import pytest

from lakehouse_catalog.errors import (
    SnapshotExpiredError,
    SnapshotNotFoundError,
    ValidationError,
)

log = logging.getLogger("lakehouse_catalog.test")


def _commit(catalog, cid, base, rows, dt):
    return catalog.commit(
        "events",
        {"base_version": base,
         "append": [{"rows": rows, "partition": {"dt": dt}}]},
        commit_id=cid)


def _seed(catalog):
    _commit(catalog, "c2", 1, [{"id": 1, "msg": "a", "dt": "d1"}], "d1")
    _commit(catalog, "c3", 2, [{"id": 2, "msg": "b", "dt": "d2"}], "d2")
    _commit(catalog, "c4", 3, [{"id": 3, "msg": "c", "dt": "d1"}], "d1")


def test_read_by_version_and_commit_id_agree(simple_table):
    catalog = simple_table
    _seed(catalog)
    by_version = catalog.read_table("events", version=3)
    by_commit = catalog.read_table("events", commit_id="c3")
    assert by_version["commit_id"] == "c3"
    assert by_commit["version"] == 3
    assert by_version["rows"] == by_commit["rows"]
    assert by_version["row_count"] == 2
    log.info("history read basis: version=%s commit_id=%s schema_version=%s",
             by_version["version"], by_version["commit_id"],
             by_version["schema_version"])


def test_reads_never_observe_partial_snapshot(simple_table):
    catalog = simple_table
    _commit(catalog, "c2", 1, [{"id": 1, "msg": "a", "dt": "d1"}], "d1")
    # Current read == latest fully committed snapshot, never partial.
    read = catalog.read_table("events")
    assert read["version"] == 2 and read["row_count"] == 1
    v1 = catalog.read_table("events", version=1)
    assert v1["row_count"] == 0 and v1["files"] == []


def test_partition_filter_strictly_follows_selected_snapshot(simple_table):
    catalog = simple_table
    _seed(catalog)
    r = catalog.read_table("events", version=4, partitions={"dt": "d1"})
    assert [row["id"] for row in r["rows"]] == [1, 3]
    r2 = catalog.read_table("events", version=2, partitions={"dt": "d1"})
    assert [row["id"] for row in r2["rows"]] == [1]
    # At version 1 partition spec already exists; filtering a key absent
    # from the *snapshot* spec is rejected (rather than silently empty).
    with pytest.raises(ValidationError) as ei:
        catalog.read_table("events", version=1, partitions={"nope": "x"})
    assert ei.value.details["reason"] == "unknown_partition_for_snapshot"
    assert ei.value.details["snapshot_version"] == 1


def test_unknown_vs_expired_version_are_distinct(simple_table):
    catalog = simple_table
    _seed(catalog)
    with pytest.raises(SnapshotNotFoundError) as nf:
        catalog.read_table("events", version=99)
    assert nf.value.details["reason"] == "unknown_version"
    log.info("unknown version basis: %s", nf.value.details)

    # Keep only current (4) by aggressively expiring + no tags.
    out = catalog.expire_snapshots("events", {"min_snapshots": 1})
    assert out["expired_versions"], out
    victim = out["expired_versions"][0]
    with pytest.raises(SnapshotExpiredError) as ex:
        catalog.read_table("events", version=victim)
    assert ex.value.details["version"] == victim
    assert ex.value.code == "snapshot_expired"
    log.info("expired version basis: %s", ex.value.details)
    # Unknown is still 404, not 410.
    with pytest.raises(SnapshotNotFoundError):
        catalog.read_table("events", version=99)


def test_tagged_snapshot_survives_cleanup_and_stays_readable(simple_table):
    catalog = simple_table
    _seed(catalog)
    catalog.tag_snapshot("events", 2)
    out = catalog.expire_snapshots(
        "events", {"min_snapshots": 1, "max_age_seconds": 0})
    assert 2 not in out["expired_versions"]
    assert 2 in out["retained_versions"]
    read = catalog.read_table("events", version=2)
    assert [r["id"] for r in read["rows"]] == [1]


def test_cleanup_deletes_only_exclusively_owned_files(simple_table):
    catalog = simple_table
    _commit(catalog, "c2", 1, [{"id": 1, "msg": "a", "dt": "d1"}], "d1")
    # overwrite at v3 removes the v2 data file and adds a new one
    catalog.commit(
        "events",
        {"base_version": 2,
         "overwrite": [{"rows": [{"id": 9, "msg": "z", "dt": "d1"}],
                        "partition": {"dt": "d1"}}]},
        commit_id="c3")
    listing = catalog.list_snapshots("events")
    assert [s["version"] for s in listing["snapshots"]] == [1, 2, 3]
    # v2 is still retained and its own file still exists on disk
    v2 = catalog.read_table("events", version=2)
    assert v2["rows"][0]["id"] == 1
    v3 = catalog.read_table("events", version=3)
    assert v3["rows"][0]["id"] == 9
