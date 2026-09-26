"""Schema evolution and compatibility decisions."""

from __future__ import annotations

import logging

import pytest

from lakehouse_catalog.errors import (
    InvalidDefaultError,
    SchemaIncompatibleError,
)

log = logging.getLogger("lakehouse_catalog.test")


def _seed(catalog):
    catalog.commit(
        "events",
        {"base_version": 1,
         "append": [{"rows": [{"id": 1, "msg": "a", "dt": "d1"},
                              {"id": 2, "msg": "b", "dt": "d1"}],
                     "partition": {"dt": "d1"}}]},
        commit_id="c2")


def test_add_column_requires_default_for_non_nullable(simple_table):
    catalog = simple_table
    _seed(catalog)
    with pytest.raises(SchemaIncompatibleError) as ei:
        catalog.commit("events", {"base_version": 2, "schema_change": {
            "action": "add_column", "name": "score", "type": "int",
            "nullable": False}}, commit_id="c3")
    assert ei.value.details["reason"] == "missing_default_for_new_column"
    log.info("incompatible basis: commit_id=%s reason=%s", "c3",
             ei.value.details)
    # rejected commit changed nothing
    assert catalog.list_snapshots("events")["current_version"] == 2


def test_add_column_default_type_mismatch(simple_table):
    catalog = simple_table
    _seed(catalog)
    with pytest.raises(InvalidDefaultError) as ei:
        catalog.commit("events", {"base_version": 2, "schema_change": {
            "action": "add_column", "name": "score", "type": "int",
            "default": "not-an-int"}}, commit_id="c3")
    assert ei.value.details["reason"] == "default_type_mismatch"


def test_add_column_on_read_backfills_history_projection(simple_table):
    catalog = simple_table
    _seed(catalog)
    out = catalog.commit("events", {"base_version": 2, "schema_change": {
        "action": "add_column", "name": "score", "type": "int",
        "default": 5, "backfill": "on_read"}}, commit_id="c3")
    assert out["schema_version"] == 2 and out["version"] == 3
    current = catalog.read_table("events")
    assert [r["score"] for r in current["rows"]] == [5, 5]
    # Old schema remains exactly as it was for the old snapshot.
    old = catalog.read_table("events", version=2)
    old_names = [c["name"] for c in old["schema"]["columns"]]
    assert "score" not in old_names
    assert "score" not in old["rows"][0]
    # New writes may override the default.
    catalog.commit("events", {"base_version": 3, "append": [
        {"rows": [{"id": 3, "msg": "c", "dt": "d1", "score": 9}],
         "partition": {"dt": "d1"}}]}, commit_id="c4")
    scores = {r["id"]: r["score"]
              for r in catalog.read_table("events")["rows"]}
    assert scores == {1: 5, 2: 5, 3: 9}


def test_add_column_materialized_backfill_rewrites_live_files(simple_table):
    catalog = simple_table
    _seed(catalog)
    before = catalog.read_table("events", version=2)
    old_file_ids = {f["file_id"] for f in before["files"]}
    out = catalog.commit("events", {"base_version": 2, "schema_change": {
        "action": "add_column", "name": "score", "type": "int",
        "default": 7, "backfill": "materialized"}}, commit_id="c3")
    assert out["version"] == 3
    snap3 = catalog.get_snapshot("events", version=3)
    # old files replaced by rewritten files (old ids removed)
    assert set(snap3["removed_files"]) >= old_file_ids
    assert not (set(snap3["file_ids"]) & old_file_ids)
    rows = catalog.read_table("events", version=3)["rows"]
    assert {r["score"] for r in rows} == {7}
    # historical snapshot v2 still uses its original (retained) file ids
    v2 = catalog.read_table("events", version=2)
    assert {f["file_id"] for f in v2["files"]} == old_file_ids
    assert "score" not in v2["rows"][0]


@pytest.mark.parametrize("change,reason", [
    ({"action": "drop_column", "name": "msg"}, "column_drop_not_supported"),
    ({"action": "rename_column", "name": "msg", "new_name": "text"},
     "column_rename_not_supported"),
    ({"action": "widen_type", "name": "id", "type": "string"},
     "incompatible_type_change"),
    ({"action": "add_column", "name": "id", "type": "int"},
     "column_already_exists"),
])
def test_incompatible_changes_rejected_with_distinct_reasons(
        simple_table, change, reason):
    catalog = simple_table
    _seed(catalog)
    with pytest.raises(SchemaIncompatibleError) as ei:
        catalog.commit("events", {"base_version": 2,
                                  "schema_change": change}, commit_id="x")
    assert ei.value.details["reason"] == reason, ei.value.details
    log.info("rejected schema change: change=%s decision=reject reason=%s",
             change.get("action"), reason)
    assert catalog.list_snapshots("events")["current_version"] == 2


def test_int_to_long_widening_is_accepted(simple_table):
    catalog = simple_table
    _seed(catalog)
    out = catalog.commit("events", {"base_version": 2, "schema_change": {
        "action": "widen_type", "name": "id", "type": "long"}},
        commit_id="c3")
    assert out["schema_version"] == 2
    rows = catalog.read_table("events")["rows"]
    assert all(isinstance(r["id"], int) for r in rows)
    # Historical snapshot still records schema v1 with int ids.
    v2 = catalog.read_table("events", version=2)
    id_col = next(c for c in v2["schema"]["columns"] if c["name"] == "id")
    assert id_col["type"] == "int"


def test_narrowing_long_to_int_is_type_narrowing(simple_table):
    catalog = simple_table
    _seed(catalog)
    catalog.commit("events", {"base_version": 2, "schema_change": {
        "action": "widen_type", "name": "id", "type": "long"}},
        commit_id="c3")
    with pytest.raises(SchemaIncompatibleError) as ei:
        catalog.commit("events", {"base_version": 3, "schema_change": {
            "action": "widen_type", "name": "id", "type": "int"}},
            commit_id="c4")
    assert ei.value.details["reason"] == "type_narrowing"
    assert ei.value.details["old_type"] == "long"
    assert ei.value.details["new_type"] == "int"


def test_rejected_evolution_writes_no_files(simple_table, tmp_path):
    catalog = simple_table
    _seed(catalog)
    data_dir = catalog.storage.data_dir("events")
    before = {p.name for p in data_dir.iterdir()}
    with pytest.raises(SchemaIncompatibleError):
        catalog.commit("events", {"base_version": 2, "schema_change": {
            "action": "drop_column", "name": "msg"}}, commit_id="bad")
    after = {p.name for p in data_dir.iterdir()}
    assert before == after
