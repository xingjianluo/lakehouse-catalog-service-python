"""HTTP entry point: status codes and error codes match the Python API."""

from __future__ import annotations


def _create(client):
    r = client.post("/tables", json={
        "name": "events",
        "columns": [
            {"name": "id", "type": "int", "nullable": False},
            {"name": "dt", "type": "string"},
        ],
        "partition_by": ["dt"],
        "commit_id": "create-1"})
    assert r.status_code == 200, r.text


def _append(client, cid, base, ids, dt="d1"):
    return client.post("/tables/events/commit", json={
        "commit_id": cid, "base_version": base,
        "append": [{"rows": [{"id": i, "dt": dt} for i in ids],
                    "partition": {"dt": dt}}]})


def test_http_create_commit_read_flow(client):
    _create(client)
    r = _append(client, "c2", 1, [1])
    assert r.status_code == 200 and r.json()["version"] == 2
    rd = client.get("/tables/events/data")
    assert rd.status_code == 200 and rd.json()["row_count"] == 1


def test_http_conflict_409_and_stale_409_distinct(client):
    _create(client)
    _append(client, "c2", 1, [1])
    # concurrent base -> 409 concurrent_commit_conflict
    r = _append(client, "c3", 1, [2])
    assert r.status_code == 409
    assert r.json()["error"] == "concurrent_commit_conflict"
    assert r.json()["reason"] == "base_version_not_current"

    # expire v1 -> committing on it becomes stale_base_version
    client.post("/tables/events/snapshots/2/tag")
    client.post("/tables/events/expire", json={"min_snapshots": 1})
    r2 = _append(client, "c4", 1, [3])
    assert r2.status_code == 409
    assert r2.json()["error"] == "stale_base_version"
    assert r2.json()["reason"] == "base_version_expired"


def test_http_duplicate_is_200_idempotent(client):
    _create(client)
    r1 = _append(client, "dup", 1, [1])
    assert r1.status_code == 200
    r2 = _append(client, "dup", 1, [2])
    assert r2.status_code == 200
    assert r2.json()["error"] == "duplicate_commit"
    assert r2.json()["version"] == 2
    # payload from retry must not have landed
    rows = client.get("/tables/events/data").json()["rows"]
    assert [r["id"] for r in rows] == [1]


def test_http_history_404_and_410(client):
    _create(client)
    _append(client, "c2", 1, [1])
    assert client.get("/tables/events/data",
                      params={"version": 99}).status_code == 404
    client.post("/tables/events/snapshots/2/tag")
    client.post("/tables/events/expire", json={"min_snapshots": 1})
    # v1 expired (v2 tagged & current -> retained), v1 is 410
    r = client.get("/tables/events/data", params={"version": 1})
    assert r.status_code == 410 and r.json()["error"] == "snapshot_expired"


def test_http_incompatible_schema_422(client):
    _create(client)
    r = client.post("/tables/events/commit", json={
        "base_version": 1,
        "schema_change": {"action": "drop_column", "name": "id"}})
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "schema_incompatible"
    assert body["reason"] == "column_drop_not_supported"


def test_http_partition_filter(client):
    _create(client)
    _append(client, "c2", 1, [1, 2], dt="d1")
    _append(client, "c3", 2, [3], dt="d2")
    r = client.get("/tables/events/data",
                   params={"version": 3, "partition": "dt=d2"})
    assert r.status_code == 200
    assert [row["id"] for row in r.json()["rows"]] == [3]


def test_http_read_by_commit_id(client):
    _create(client)
    _append(client, "c2", 1, [7])
    r = client.get("/tables/events/data", params={"commit_id": "c2"})
    assert r.status_code == 200 and r.json()["version"] == 2
    r2 = client.get("/tables/events/snapshots/c2", params={"by": "commit"})
    assert r2.status_code == 200 and r2.json()["commit_id"] == "c2"
