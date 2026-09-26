"""目录服务测试：并发提交冲突、幂等重试、快照隔离、历史读取与清理。

日志约定：每个用例打印 commit_id、snapshot_id 与判定/拒绝依据（reason）。
"""

import logging
import threading
from pathlib import Path

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    CommitConflictError,
    SnapshotNotFoundError,
    TableAlreadyExistsError,
)
from lakehouse.models import Column, Schema

log = logging.getLogger("lakehouse.tests.catalog")

SCHEMA = Schema((Column("id", "long"), Column("v", "int")))


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    c = Catalog(tmp_path / "wh", retain_last=2)
    c.create_table("t", SCHEMA)
    return c


def _commit_one(c: Catalog, commit_id: str, row_id: int, base=None) -> int:
    tx = c.begin_commit("t")
    if base is not None:
        tx.base_snapshot_id = base
    tx.add_partition({"date": "2026-09-26"}, [{"id": row_id, "v": row_id * 10}])
    snap = c.commit(tx, commit_id=commit_id)
    log.info("提交成功: commit_id=%s snapshot_id=%s base=%s",
             commit_id, snap.snapshot_id, tx.base_snapshot_id)
    return snap.snapshot_id


def test_concurrent_commits_exactly_one_wins(catalog: Catalog):
    """N 个线程基于同一基版本并发提交：恰好一个成功，其余 commit_conflict。"""
    base_sid = _commit_one(catalog, "seed", 0)
    n = 8
    results: dict[str, object] = {}
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        cid = f"race-{i}"
        tx = catalog.begin_commit("t")  # 所有线程都读到 base_sid
        tx.add_partition({"date": f"d{i}"}, [{"id": i, "v": i}])
        barrier.wait()  # 同时进入提交临界区
        try:
            snap = catalog.commit(tx, commit_id=cid)
            results[cid] = ("committed", snap.snapshot_id)
        except CommitConflictError as e:
            results[cid] = ("rejected", e.reason, dict(e.detail))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    committed = {k: v for k, v in results.items() if v[0] == "committed"}
    rejected = {k: v for k, v in results.items() if v[0] == "rejected"}
    for cid, r in sorted(results.items()):
        log.info("并发结果: commit_id=%s -> %s", cid, r)
    assert len(committed) == 1, "并发提交必须恰好一个整体生效"
    assert len(rejected) == n - 1
    assert all(r[1] == "commit_conflict" for r in rejected.values())

    # 元数据一致：当前快照是胜者的快照，且父链连续。
    current = catalog.current_snapshot("t")
    winner_sid = next(iter(committed.values()))[1]
    log.info("判定依据: current_snapshot=%s 胜者 snapshot=%s base=%s",
             current.snapshot_id, winner_sid, base_sid)
    assert current.snapshot_id == winner_sid
    assert current.parent_snapshot_id == base_sid
    # 读者只能看到完整快照：行数 = 种子 1 行 + 胜者 1 行。
    assert len(catalog.read_rows("t")) == 2


def test_commit_retry_is_idempotent(catalog: Catalog):
    """相同 commit_id 重试返回首次结果，不产生新快照。"""
    sid1 = _commit_one(catalog, "k1", 1)
    tx = catalog.begin_commit("t")  # 重试方可能已读到更新版本
    snap = catalog.commit(tx, commit_id="k1")
    log.info("幂等重试: commit_id=k1 首次 snapshot=%s 重试返回 snapshot=%s",
             sid1, snap.snapshot_id)
    assert snap.snapshot_id == sid1
    assert catalog.current_snapshot("t").snapshot_id == sid1
    assert len(catalog.read_rows("t")) == 1

    # 不同 commit_id 的重复提交不是幂等重试，应按冲突拒绝。
    sid2 = _commit_one(catalog, "k2", 2)
    tx = catalog.begin_commit("t")
    tx.base_snapshot_id = sid1  # 过期基版本
    tx.add_partition({"date": "d"}, [{"id": 3, "v": 3}])
    with pytest.raises(CommitConflictError) as exc:
        catalog.commit(tx, commit_id="k3")
    log.info("过期基版本拒绝: commit_id=k3 reason=%s detail=%s",
             exc.value.reason, exc.value.detail)
    assert exc.value.reason == "commit_conflict"
    assert catalog.current_snapshot("t").snapshot_id == sid2


def test_rejected_commit_recorded_and_queryable(catalog: Catalog):
    sid1 = _commit_one(catalog, "c1", 1)
    tx = catalog.begin_commit("t")
    tx.base_snapshot_id = None  # 过期
    tx.add_partition({"date": "d"}, [{"id": 2, "v": 2}])
    with pytest.raises(CommitConflictError):
        catalog.commit(tx, commit_id="c2")
    # 同一 commit_id 再试仍得到相同的拒绝结论（审计可回放）。
    tx2 = catalog.begin_commit("t")
    tx2.add_partition({"date": "d"}, [{"id": 2, "v": 2}])
    with pytest.raises(CommitConflictError) as exc:
        catalog.commit(tx2, commit_id="c2")
    log.info("拒绝记录回放: commit_id=c2 reason=%s（当前版本 %s）",
             exc.value.reason, sid1)
    # 按提交标识读取被拒绝的提交 -> 明确拒绝。
    with pytest.raises(SnapshotNotFoundError) as exc2:
        catalog.get_snapshot_by_commit("t", "c2")
    assert exc2.value.reason == "snapshot_not_found"
    log.info("按提交标识读取被拒提交: reason=%s", exc2.value.reason)


def test_snapshot_isolation_and_history_read(catalog: Catalog):
    sid1 = _commit_one(catalog, "h1", 1)
    sid2 = _commit_one(catalog, "h2", 2)
    # 历史快照分区与数据严格一致。
    assert catalog.read_rows("t", sid1) == [{"id": 1, "v": 10}]
    parts1 = catalog.list_partitions("t", sid1)
    parts2 = catalog.list_partitions("t", sid2)
    log.info("历史读取: snapshot=%s 分区=%d 个; snapshot=%s 分区=%d 个",
             sid1, len(parts1), sid2, len(parts2))
    assert len(parts1) == 1 and len(parts2) == 2
    # 按提交标识读取历史。
    assert catalog.get_snapshot_by_commit("t", "h1").snapshot_id == sid1
    # 不存在的快照/提交被明确拒绝。
    with pytest.raises(SnapshotNotFoundError):
        catalog.get_snapshot("t", 999)
    with pytest.raises(SnapshotNotFoundError):
        catalog.get_snapshot_by_commit("t", "no-such-commit")


def test_schema_evolution_during_concurrent_commit(catalog: Catalog):
    """演进提交与数据提交并发：演进成功后，过期基版本的数据提交被拒；
    任一时刻读到的都是完整结构（无中间态）。"""
    sid1 = _commit_one(catalog, "e1", 1)
    evolved = Schema(SCHEMA.columns + (Column("tag", "string", default="n/a", has_default=True),))

    tx_evo = catalog.begin_commit("t")
    tx_evo.set_schema(evolved)
    tx_data = catalog.begin_commit("t")
    tx_data.add_partition({"date": "d"}, [{"id": 2, "v": 20}])

    snap_evo = catalog.commit(tx_evo, commit_id="evo")
    log.info("演进提交: commit_id=evo snapshot=%s 新增列 tag 默认值 'n/a'",
             snap_evo.snapshot_id)
    with pytest.raises(CommitConflictError) as exc:
        catalog.commit(tx_data, commit_id="data-late")
    log.info("并发数据提交被拒: commit_id=data-late reason=%s base=%s current=%s",
             exc.value.reason, sid1, snap_evo.snapshot_id)

    # 演进后读取：历史行回填默认值；历史快照仍是旧结构。
    rows = catalog.read_rows("t")
    assert rows == [{"id": 1, "v": 10, "tag": "n/a"}]
    assert catalog.read_rows("t", sid1) == [{"id": 1, "v": 10}]
    log.info("回填验证: 当前快照行=%s；历史快照 %s 保持旧结构", rows, sid1)


def test_incompatible_evolution_rejected_atomically(catalog: Catalog):
    sid1 = _commit_one(catalog, "s1", 1)
    tx = catalog.begin_commit("t")
    tx.set_schema(Schema((Column("id", "long"),)))  # 删列
    tx.add_partition({"date": "d"}, [{"id": 2}])
    with pytest.raises(Exception) as exc:
        catalog.commit(tx, commit_id="bad-evo")
    reason = getattr(exc.value, "reason", None)
    log.info("不兼容演进拒绝: commit_id=bad-evo reason=%s snapshot 保持 %s",
             reason, sid1)
    assert reason == "schema_column_dropped"
    # 整体失败：表指针未移动，暂存数据不可见。
    assert catalog.current_snapshot("t").snapshot_id == sid1
    assert catalog.read_rows("t") == [{"id": 1, "v": 10}]


def test_cleanup_respects_pins_and_retain_window(catalog: Catalog):
    sids = [_commit_one(catalog, f"g{i}", i) for i in range(5)]
    catalog.pin_snapshot("t", sids[0])
    removed = catalog.cleanup("t")
    log.info("清理: 快照序列=%s pin=%s retain_last=2 -> 回收=%s",
             sids, sids[0], removed)
    # pinned 的首个快照必须保留且可读。
    assert sids[0] not in removed
    assert catalog.read_rows("t", sids[0]) == [{"id": 0, "v": 0}]
    # 未 pin 且超出保留窗口的被回收，读取明确拒绝。
    assert sids[1] in removed and sids[2] in removed
    with pytest.raises(SnapshotNotFoundError) as exc:
        catalog.get_snapshot("t", sids[1])
    log.info("已回收快照读取: snapshot=%s reason=%s", sids[1], exc.value.reason)
    assert exc.value.reason == "snapshot_not_found"
    # 保留窗口内与当前快照可读。
    assert catalog.read_rows("t", sids[-1])
    # 解除 pin 后可被回收。
    catalog.unpin_snapshot("t", sids[0])
    removed2 = catalog.cleanup("t")
    log.info("解除 pin 后再次清理: 回收=%s", removed2)
    assert sids[0] in removed2


def test_duplicate_table_rejected(catalog: Catalog):
    with pytest.raises(TableAlreadyExistsError) as exc:
        catalog.create_table("t", SCHEMA)
    log.info("重复建表拒绝: reason=%s", exc.value.reason)
    assert exc.value.reason == "table_already_exists"
