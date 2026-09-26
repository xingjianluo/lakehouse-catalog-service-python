"""Retention cleanup and durability across catalog restarts."""

from __future__ import annotations

from lakehouse_catalog.catalog import LakehouseCatalog


def _append(catalog, cid, base, i, dt="d1"):
    return catalog.commit(
        "events",
        {"base_version": base,
         "append": [{"rows": [{"id": i, "msg": f"m{i}", "dt": dt}],
                     "partition": {"dt": dt}}]},
        commit_id=cid)


def test_history_survives_catalog_restart(warehouse):
    cat = LakehouseCatalog(warehouse, require_explicit_base=True)
    cat.create_table("events", [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "msg", "type": "string"},
        {"name": "dt", "type": "string"}],
        partition_by=["dt"], commit_id="create")
    _append(cat, "c2", 1, 1)
    _append(cat, "c3", 2, 2)

    cat2 = LakehouseCatalog(warehouse, require_explicit_base=True)
    read = cat2.read_table("events", commit_id="c2")
    assert read["rows"][0]["id"] == 1
    assert cat2.read_table("events", version=3)["row_count"] == 2
    # ledger survived: duplicate c2 recognized across restarts
    import pytest

    from lakehouse_catalog.errors import DuplicateCommitError
    with pytest.raises(DuplicateCommitError) as ei:
        _append(cat2, "c2", 1, 9)
    assert ei.value.details["version"] == 2


def test_cleanup_keeps_referenced_versions_only(warehouse):
    cat = LakehouseCatalog(warehouse, require_explicit_base=True)
    cat.create_table("events", [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "msg", "type": "string"},
        {"name": "dt", "type": "string"}],
        partition_by=["dt"], commit_id="create")
    for i in range(2, 8):
        _append(cat, f"c{i}", i - 1, i)
    # pin v3; default retention keeps the newest 5 -> retained includes 3..7
    cat.tag_snapshot("events", 3)
    out = cat.expire_snapshots("events")
    assert out["expired_versions"] == [1, 2]
    assert 3 in out["retained_versions"]
    # pinned version still fully readable with its files on disk;
    # v3 contains the rows from commits c2 and c3 (ids 2 and 3)
    rows = cat.read_table("events", version=3)["rows"]
    assert sorted(r["id"] for r in rows) == [2, 3]
    # expired versions are tombstones after restart too
    cat2 = LakehouseCatalog(warehouse)
    listing = cat2.list_snapshots("events")
    assert set(listing["expired_versions"]) >= {1, 2}
    import pytest

    from lakehouse_catalog.errors import SnapshotExpiredError
    with pytest.raises(SnapshotExpiredError):
        cat2.read_table("events", version=1)


def test_version_records_removed_on_expire_but_ledger_remains(warehouse):
    cat = LakehouseCatalog(warehouse, require_explicit_base=True)
    cat.create_table("events", [{"name": "id", "type": "int"}],
                     commit_id="create")
    cat.commit("events", {"base_version": 1,
        "append": [{"rows": [{"id": 1}]}]}, commit_id="c2")
    cat.expire_snapshots("events", {"min_snapshots": 1})
    assert not cat.storage.version_path("events", 1).exists()
    assert cat.storage.ledger_path("events").exists()
    entries = cat._ledger("events").all_entries()
    assert {e.version for e in entries} == {1, 2}


def test_overwrite_reclaims_old_files_only_after_new_snapshot_committed(warehouse):
    cat = LakehouseCatalog(warehouse, require_explicit_base=True)
    cat.create_table("events", [
        {"name": "id", "type": "int"}, {"name": "msg", "type": "string"},
        {"name": "dt", "type": "string"}],
        partition_by=["dt"], commit_id="create")
    _append(cat, "c2", 1, 1)
    old_fid = cat.read_table("events", version=2)["files"][0]["file_id"]
    cat.commit("events", {"base_version": 2, "overwrite": [
        {"rows": [{"id": 9, "dt": "d1"}], "partition": {"dt": "d1"}}]},
        commit_id="c3")
    # v2 still readable: old file retained while referenced
    assert cat.storage.data_path("events", old_fid).exists()
    # cleanup removing v2 finally deletes the now-unreferenced file
    cat.expire_snapshots("events", {"min_snapshots": 1})
    assert not cat.storage.data_path("events", old_fid).exists()


def test_materialized_backfill_files_gced_only_when_unreferenced(warehouse):
    cat = LakehouseCatalog(warehouse, require_explicit_base=True)
    cat.create_table("events", [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "dt", "type": "string"}],
        partition_by=["dt"], commit_id="create")
    cat.commit("events", {"base_version": 1, "append": [
        {"rows": [{"id": 1, "dt": "d1"}], "partition": {"dt": "d1"}}]},
        commit_id="c2")
    v2_files = {f["file_id"]
                for f in cat.read_table("events", version=2)["files"]}
    # materialized add-column at v3: old files replaced by rewritten ones
    cat.commit("events", {"base_version": 2, "schema_change": {
        "action": "add_column", "name": "score", "type": "int",
        "default": 0, "backfill": "materialized"}}, commit_id="c3")
    v3_files = {f["file_id"]
                for f in cat.read_table("events", version=3)["files"]}
    assert v2_files.isdisjoint(v3_files)
    # v2 retained -> original files still on disk
    for fid in v2_files:
        assert cat.storage.data_path("events", fid).exists()
    # expire everything but v3 -> old v2 files deleted, rewritten kept
    out = cat.expire_snapshots("events", {"min_snapshots": 1})
    assert 2 in out["expired_versions"]
    for fid in v2_files:
        assert not cat.storage.data_path("events", fid).exists()
    for fid in v3_files:
        assert cat.storage.data_path("events", fid).exists()
    assert cat.read_table("events", version=3)["rows"][0]["score"] == 0
