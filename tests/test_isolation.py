"""Snapshot isolation: readers never observe an intermediate state.

While a schema-evolution commit is in flight, every reader must see either
the complete old snapshot (old schema + old files) or the complete new
snapshot (new schema + all files with the default projected). A half-applied
mix (new schema over partially rewritten files) must never be visible.
"""

from __future__ import annotations

import threading

from lakehouse_catalog.errors import ConcurrentCommitConflictError


def test_reader_during_materialized_evolution_sees_consistent_snapshot(simple_table):
    catalog = simple_table
    # Seed several data files across versions.
    for i, v in ((10, 1), (20, 2), (30, 3)):
        catalog.commit(
            "events",
            {"base_version": v,
             "append": [{"rows": [{"id": i, "msg": f"m{i}", "dt": "d1"}],
                         "partition": {"dt": "d1"}}]},
            commit_id=f"c{v + 1}")
    assert catalog.read_table("events")["version"] == 4

    stop = threading.Event()
    observations: list[tuple[int, bool]] = []

    def reader() -> None:
        while not stop.is_set():
            r = catalog.read_table("events")
            cols = [c["name"] for c in r["schema"]["columns"]]
            has_score = "score" in cols
            if has_score:
                # New snapshot: EVERY row must already carry the default.
                assert all("score" in row for row in r["rows"]), r
                assert all(row["score"] == 0 for row in r["rows"])
            else:
                # Old snapshot: no row may carry the new column.
                assert all("score" not in row for row in r["rows"])
            observations.append((r["version"], has_score))

    th = threading.Thread(target=reader)
    th.start()

    def _evolve() -> None:
        catalog.commit(
            "events",
            {"base_version": 4, "schema_change": {
                "action": "add_column", "name": "score", "type": "int",
                "default": 0, "backfill": "materialized"}},
            commit_id="evolve")

    # Hammer evolution attempts concurrently with reads; only one wins and
    # readers must always observe a coherent version.
    evolve_thread = threading.Thread(target=_evolve)
    evolve_thread.start()
    evolve_thread.join()
    stop.set()
    th.join()

    assert observations, "reader never ran"
    final = catalog.read_table("events")
    assert final["version"] == 5
    assert "score" in [c["name"] for c in final["schema"]["columns"]]
    # observed versions must be a prefix of 4s then 5s, never out of order
    versions_seen = [v for v, _ in observations]
    if 4 in versions_seen and 5 in versions_seen:
        first5 = versions_seen.index(5)
        assert all(v == 4 for v in versions_seen[:first5])
        assert all(v == 5 for v in versions_seen[first5:])


def test_concurrent_evolution_commits_only_one_takes_effect(simple_table):
    catalog = simple_table
    catalog.commit(
        "events",
        {"base_version": 1,
         "append": [{"rows": [{"id": 1, "msg": "a", "dt": "d1"}],
                     "partition": {"dt": "d1"}}]},
        commit_id="seed")
    barrier = threading.Barrier(3)
    outcomes: list[str] = []

    def evolve(cid: str, col: str) -> None:
        barrier.wait()
        try:
            catalog.commit(
                "events",
                {"base_version": 2, "schema_change": {
                    "action": "add_column", "name": col,
                    "type": "int", "default": 0}},
                commit_id=cid)
            outcomes.append(cid)
        except ConcurrentCommitConflictError:
            outcomes.append("conflict")

    threads = [
        threading.Thread(target=evolve, args=("e1", "a_col")),
        threading.Thread(target=evolve, args=("e2", "b_col")),
        threading.Thread(target=evolve, args=("e3", "c_col")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len([o for o in outcomes if o != "conflict"]) == 1
    final = catalog.read_table("events")
    added = next(o for o in outcomes if o != "conflict")
    expected_col = {"e1": "a_col", "e2": "b_col", "e3": "c_col"}[added]
    assert expected_col in [c["name"] for c in final["schema"]["columns"]]
    # Exactly one schema version beyond v1 exists (no intermediate stacking).
    snap = catalog.list_snapshots("events")
    assert snap["current_version"] == 3
