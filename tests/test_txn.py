"""跨表事务测试：原子提交、并发冲突、幂等重放、一致读取与不可用版本拒绝。

日志约定：每个用例打印 txn_id、涉及各表的 snapshot_id 与判定/拒绝依据（reason）。
"""

import logging
import threading
from pathlib import Path

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    TxnConflictError,
    TxnNotCommittedError,
    TxnNotFoundError,
    TxnSnapshotUnavailableError,
    TxnTableNotInvolvedError,
)
from lakehouse.models import Column, Schema

log = logging.getLogger("lakehouse.tests.txn")

SCHEMA = Schema((Column("id", "long"), Column("v", "int")))


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    c = Catalog(tmp_path / "wh", retain_last=1)
    for t in ("detail", "summary"):
        c.create_table(t, SCHEMA)
    return c


def _seed(c: Catalog, table: str, commit_id: str, row_id: int) -> int:
    tx = c.begin_commit(table)
    tx.add_partition({"date": "seed"}, [{"id": row_id, "v": row_id}])
    snap = c.commit(tx, commit_id=commit_id)
    log.info("种子提交: table=%s commit_id=%s snapshot_id=%s", table, commit_id, snap.snapshot_id)
    return snap.snapshot_id


def _txn_append(c: Catalog, txn_id: str, adds: dict[str, list[dict]]) -> dict[str, int]:
    """开启跨表事务，为每张表追加一行，提交并返回 {table: snapshot_id}。"""
    txn = c.begin_txn(list(adds))
    for table, rows in adds.items():
        txn.change(table).add_partition({"date": txn_id}, rows)
    snaps = c.commit_txn(txn, txn_id=txn_id)
    result = {t: s.snapshot_id for t, s in snaps.items()}
    log.info("事务提交成功: txn_id=%s 各表快照=%s", txn_id, result)
    return result


def test_txn_commit_all_or_nothing(catalog: Catalog):
    """一笔事务同时改明细表与汇总表：两表同时生效，快照组互相自洽。"""
    _seed(catalog, "detail", "seed-d", 1)
    _seed(catalog, "summary", "seed-s", 10)

    sids = _txn_append(catalog, "txn-1", {
        "detail": [{"id": 2, "v": 20}],
        "summary": [{"id": 11, "v": 110}],
    })

    # 两表当前版本都来自 txn-1，且都能读到各自的新增行。
    for table, row_cnt in (("detail", 2), ("summary", 2)):
        cur = catalog.current_snapshot(table)
        log.info("自洽检查: table=%s current_snapshot=%s commit_id=%s",
                 table, cur.snapshot_id, cur.commit_id)
        assert cur.snapshot_id == sids[table]
        assert cur.commit_id == "txn-1"
        assert len(catalog.read_rows(table)) == row_cnt

    # 按事务读取整组快照：与提交时返回的完全一致。
    group = catalog.get_txn_snapshots("txn-1")
    assert {t: s.snapshot_id for t, s in group.items()} == sids
    log.info("按事务读取: txn_id=txn-1 快照组=%s（与提交返回一致）",
             {t: s.snapshot_id for t, s in group.items()})


def test_txn_rejected_when_any_base_stale(catalog: Catalog):
    """任一表基版本过期 -> 整笔拒绝，逐表原因可见，未过期的表也不生效。"""
    seed_d = _seed(catalog, "detail", "seed-d", 1)
    seed_s = _seed(catalog, "summary", "seed-s", 10)

    txn = catalog.begin_txn(["detail", "summary"])
    txn.change("detail").base_snapshot_id = None  # 过期基版本
    txn.change("detail").add_partition({"date": "x"}, [{"id": 2, "v": 20}])
    txn.change("summary").add_partition({"date": "x"}, [{"id": 11, "v": 110}])
    with pytest.raises(TxnConflictError) as exc:
        catalog.commit_txn(txn, txn_id="txn-stale")
    failures = exc.value.detail["failures"]
    log.info("整笔拒绝: txn_id=txn-stale 逐表原因=%s", failures)
    assert exc.value.reason == "txn_conflict"
    assert set(failures) == {"detail"}
    assert failures["detail"]["reason"] == "commit_conflict"
    assert failures["detail"]["detail"]["expected_base"] == seed_d

    # 全部表保持原版本：不允许部分生效。
    assert catalog.current_snapshot("detail").snapshot_id == seed_d
    assert catalog.current_snapshot("summary").snapshot_id == seed_s
    assert len(catalog.read_rows("detail")) == 1
    assert len(catalog.read_rows("summary")) == 1
    log.info("判定依据: detail 仍为 snapshot=%s, summary 仍为 snapshot=%s（无部分生效）",
             seed_d, seed_s)


def test_concurrent_txns_exactly_one_wins(catalog: Catalog):
    """N 个线程对同一批表并发提交事务：恰好一笔整体生效，其余整笔拒绝。"""
    _seed(catalog, "detail", "seed-d", 1)
    _seed(catalog, "summary", "seed-s", 10)
    n = 6
    results: dict[str, object] = {}
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        txn_id = f"txn-race-{i}"
        txn = catalog.begin_txn(["detail", "summary"])
        txn.change("detail").add_partition({"date": f"d{i}"}, [{"id": i, "v": i}])
        txn.change("summary").add_partition({"date": f"d{i}"}, [{"id": i, "v": i}])
        barrier.wait()
        try:
            snaps = catalog.commit_txn(txn, txn_id=txn_id)
            results[txn_id] = ("committed", {t: s.snapshot_id for t, s in snaps.items()})
        except TxnConflictError as e:
            results[txn_id] = ("rejected", sorted(e.detail["failures"]))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    committed = {k: v for k, v in results.items() if v[0] == "committed"}
    rejected = {k: v for k, v in results.items() if v[0] == "rejected"}
    for txn_id, r in sorted(results.items()):
        log.info("并发事务结果: txn_id=%s -> %s", txn_id, r)
    assert len(committed) == 1, "并发事务必须恰好一笔整体生效"
    assert len(rejected) == n - 1
    # 被拒绝的事务两张表都报 commit_conflict（两表基版本都过期）。
    assert all(r[1] == ["detail", "summary"] for r in rejected.values())

    # 胜者两表同时前进一步，且当前快照都标记为胜者的 txn_id。
    winner_id, (_, winner_sids) = next(iter(committed.items()))
    for table in ("detail", "summary"):
        cur = catalog.current_snapshot(table)
        log.info("胜者校验: table=%s current_snapshot=%s commit_id=%s",
                 table, cur.snapshot_id, cur.commit_id)
        assert cur.snapshot_id == winner_sids[table]
        assert cur.commit_id == winner_id
        assert len(catalog.read_rows(table)) == 2  # 种子 1 行 + 胜者 1 行


def test_txn_retry_is_idempotent(catalog: Catalog):
    """同一 txn_id 重试：成功返回同一组快照，失败返回相同原因，不产生新版本。"""
    _seed(catalog, "detail", "seed-d", 1)
    _seed(catalog, "summary", "seed-s", 10)
    sids = _txn_append(catalog, "txn-k", {
        "detail": [{"id": 2, "v": 20}],
        "summary": [{"id": 11, "v": 110}],
    })

    # 成功重放：返回完全相同的快照组，两表版本不再前进。
    txn = catalog.begin_txn(["detail", "summary"])
    txn.change("detail").add_partition({"date": "dup"}, [{"id": 99, "v": 99}])
    replay = catalog.commit_txn(txn, txn_id="txn-k")
    replay_sids = {t: s.snapshot_id for t, s in replay.items()}
    log.info("幂等重放: txn_id=txn-k 首次=%s 重放=%s", sids, replay_sids)
    assert replay_sids == sids
    assert catalog.current_snapshot("detail").snapshot_id == sids["detail"]
    assert catalog.current_snapshot("summary").snapshot_id == sids["summary"]
    assert len(catalog.read_rows("detail")) == 2  # 重放不产生新数据

    # 失败重放：构造一笔被拒绝的事务，再以相同 txn_id 重试。
    bad = catalog.begin_txn(["detail", "summary"])
    bad.change("detail").base_snapshot_id = None
    bad.change("detail").add_partition({"date": "y"}, [{"id": 3, "v": 30}])
    with pytest.raises(TxnConflictError) as first:
        catalog.commit_txn(bad, txn_id="txn-bad")
    retry = catalog.begin_txn(["detail", "summary"])
    retry.change("detail").add_partition({"date": "y"}, [{"id": 3, "v": 30}])
    with pytest.raises(TxnConflictError) as second:
        catalog.commit_txn(retry, txn_id="txn-bad")
    log.info("失败重放: txn_id=txn-bad 首次=%s 重试=%s",
             first.value.detail["failures"], second.value.detail["failures"])
    assert second.value.detail == first.value.detail


def test_txn_table_order_irrelevant(tmp_path: Path):
    """事务里表的书写顺序不同，最终结果一致。"""
    results = []
    for order in (["detail", "summary"], ["summary", "detail"]):
        c = Catalog(tmp_path / f"wh-{order[0]}", retain_last=1)
        for t in ("detail", "summary"):
            c.create_table(t, SCHEMA)
            _seed(c, t, f"seed-{t}", 1)
        txn = c.begin_txn(order)
        for t in order:
            txn.change(t).add_partition({"date": "x"}, [{"id": 2, "v": 20}])
        snaps = c.commit_txn(txn, txn_id="txn-ord")
        results.append(({t: s.snapshot_id for t, s in snaps.items()},
                        {t: c.read_rows(t) for t in order}))
    log.info("顺序无关: [detail,summary] -> %s; [summary,detail] -> %s", *results)
    assert results[0] == results[1]


def test_txn_read_consistent_group(catalog: Catalog):
    """按事务读取：整组快照互相自洽；未参与的表/未知事务/被拒事务分别拒绝。"""
    catalog.create_table("other", SCHEMA)
    _seed(catalog, "detail", "seed-d", 1)
    _seed(catalog, "summary", "seed-s", 10)
    sids = _txn_append(catalog, "txn-g", {
        "detail": [{"id": 2, "v": 20}],
        "summary": [{"id": 11, "v": 110}],
    })

    group = catalog.get_txn_snapshots("txn-g")
    for t, snap in group.items():
        # 组内每张表的快照都由 txn-g 产生 -> 都能看到这笔事务的全部变更。
        assert snap.commit_id == "txn-g"
        assert snap.snapshot_id == sids[t]
        assert len(catalog.read_rows(t, snap.snapshot_id)) == 2
    log.info("一致读取: txn_id=txn-g 快照组=%s 均含本事务变更", sids)

    with pytest.raises(TxnTableNotInvolvedError) as e1:
        catalog.get_txn_snapshot("txn-g", "other")
    log.info("未参与表拒绝: table=other reason=%s", e1.value.reason)
    assert e1.value.reason == "txn_table_not_involved"

    with pytest.raises(TxnNotFoundError) as e2:
        catalog.get_txn_snapshots("txn-no-such")
    log.info("未知事务拒绝: reason=%s", e2.value.reason)
    assert e2.value.reason == "txn_not_found"

    bad = catalog.begin_txn(["detail", "summary"])
    bad.change("detail").base_snapshot_id = None
    with pytest.raises(TxnConflictError):
        catalog.commit_txn(bad, txn_id="txn-rej")
    with pytest.raises(TxnNotCommittedError) as e3:
        catalog.get_txn_snapshots("txn-rej")
    log.info("被拒事务读取: reason=%s failures=%s", e3.value.reason, e3.value.detail["failures"])
    assert e3.value.reason == "txn_not_committed"
    assert "detail" in e3.value.detail["failures"]


def test_txn_read_unavailable_after_cleanup(catalog: Catalog):
    """事务快照被清理回收后，按事务读取明确拒绝，且与普通读取失败区分。"""
    _seed(catalog, "detail", "seed-d", 1)
    _seed(catalog, "summary", "seed-s", 10)
    sids = _txn_append(catalog, "txn-old", {
        "detail": [{"id": 2, "v": 20}],
        "summary": [{"id": 11, "v": 110}],
    })
    # detail 继续前进两个版本，把 txn-old 的快照挤出保留窗口（retain_last=1）。
    for i in range(2):
        tx = catalog.begin_commit("detail")
        tx.add_partition({"date": f"later-{i}"}, [{"id": 100 + i, "v": 100 + i}])
        snap = catalog.commit(tx, commit_id=f"later-{i}")
        log.info("后续单表提交: table=detail commit_id=later-%s snapshot=%s", i, snap.snapshot_id)
    removed = catalog.cleanup("detail")
    log.info("清理: table=detail 回收快照=%s（txn-old 的 snapshot=%s 在内）",
             removed, sids["detail"])
    assert sids["detail"] in removed

    # 按事务读取：detail 快照已回收 -> txn_snapshot_unavailable（区别于普通 404）。
    with pytest.raises(TxnSnapshotUnavailableError) as exc:
        catalog.get_txn_snapshots("txn-old")
    log.info("不可用版本拒绝: txn_id=txn-old reason=%s detail=%s",
             exc.value.reason, exc.value.detail)
    assert exc.value.reason == "txn_snapshot_unavailable"
    assert exc.value.detail["table"] == "detail"
    assert exc.value.detail["snapshot_id"] == sids["detail"]

    # 普通路径读取同一快照仍是 snapshot_not_found，两种失败可区分。
    from lakehouse.errors import SnapshotNotFoundError
    with pytest.raises(SnapshotNotFoundError) as exc2:
        catalog.get_snapshot("detail", sids["detail"])
    log.info("普通读取失败: reason=%s（与 %s 可区分）", exc2.value.reason, exc.value.reason)
    assert exc2.value.reason == "snapshot_not_found"
