"""跨表事务测试：原子生效、并发冲突、幂等重放、一致性读取与不可用版本拒绝。

日志约定：每个用例打印 txn_id、各表 snapshot_id 与判定/拒绝依据（reason）。
"""

import logging
import threading
from pathlib import Path

import pytest

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    SnapshotNotFoundError,
    TransactionConflictError,
    TransactionNotFoundError,
    TransactionSnapshotUnavailableError,
)
from lakehouse.models import Column, Schema
from lakehouse.transaction import MultiTableTransaction

log = logging.getLogger("lakehouse.tests.transaction")

SCHEMA = Schema((Column("id", "long"), Column("v", "int")))
TABLES = ("detail", "summary")


@pytest.fixture()
def catalog(tmp_path: Path) -> Catalog:
    c = Catalog(tmp_path / "wh", retain_last=2)
    for t in TABLES:
        c.create_table(t, SCHEMA)
    return c


def _current_bases(c: Catalog) -> dict[str, int | None]:
    bases = {}
    for t in TABLES:
        meta = c._load_table_meta(t)
        bases[t] = meta["current_snapshot_id"]
    return bases


def _build_txn(txn_id: str, bases: dict[str, int | None], tag: str,
               order=TABLES) -> MultiTableTransaction:
    """构造一笔覆盖两张表的事务：明细表加一行，汇总表加一行。"""
    txn = MultiTableTransaction(txn_id)
    for t in order:
        change = txn.table(t, bases[t])
        change.add_partition({"date": "2026-10-07"}, [{"id": tag, "v": 1}]
                             if t == "detail" else [{"id": tag, "v": 1}])
    return txn


def _log_result(txn_id: str, result) -> None:
    versions = {t: s.snapshot_id for t, s in result.snapshots.items()}
    log.info("事务提交成功: txn_id=%s 各表版本=%s", txn_id, versions)


def test_multi_table_commit_atomic_and_consistent_read(catalog: Catalog):
    """一笔事务覆盖两张表：同时生效，按 txn_id 读到的整组快照互相自洽。"""
    bases = _current_bases(catalog)
    txn = _build_txn("txn-1", bases, "a")
    result = catalog.commit_tables(txn)
    _log_result("txn-1", result)

    # 两张表都前进到新版本，且都能读到这笔事务的数据。
    for t in TABLES:
        current = catalog.current_snapshot(t)
        assert current.snapshot_id == result.snapshots[t].snapshot_id
        assert current.commit_id == "txn-1"
        rows = catalog.read_rows(t)
        assert rows == [{"id": "a", "v": 1}]
        log.info("单表视角: table=%s current_snapshot=%s rows=%s",
                 t, current.snapshot_id, rows)

    # 按事务读取：整组快照与提交结果一致（自洽）。
    group = catalog.transaction_snapshots("txn-1")
    assert {t: s.snapshot_id for t, s in group.items()} == \
        {t: s.snapshot_id for t, s in result.snapshots.items()}
    log.info("跨表一致读取: txn_id=txn-1 整组版本=%s",
             {t: s.snapshot_id for t, s in group.items()})


def test_stale_base_on_one_table_rejects_whole_transaction(catalog: Catalog):
    """任一表基版本过期 -> 整笔拒绝，另一张表保持原版本，且能看出各表原因。"""
    ok = catalog.commit_tables(_build_txn("txn-seed", _current_bases(catalog), "s"))
    _log_result("txn-seed", ok)

    bases = _current_bases(catalog)
    bases["summary"] = None  # summary 声明了过期基版本
    txn = _build_txn("txn-bad", bases, "x")
    with pytest.raises(TransactionConflictError) as exc:
        catalog.commit_tables(txn)
    e = exc.value
    log.info("整笔拒绝: txn_id=txn-bad reason=%s 各表原因=%s",
             e.reason, {t: f["reason"] for t, f in e.detail["tables"].items()})
    assert e.reason == "transaction_conflict"
    # 能看出是哪张表、因为什么没提交成功。
    assert set(e.detail["tables"]) == {"summary"}
    assert e.detail["tables"]["summary"]["reason"] == "commit_conflict"
    assert e.detail["tables"]["summary"]["detail"]["expected_base"] == \
        ok.snapshots["summary"].snapshot_id

    # 全部不生效：两张表都停在 txn-seed 的版本。
    for t in TABLES:
        assert catalog.current_snapshot(t).snapshot_id == ok.snapshots[t].snapshot_id
        assert catalog.read_rows(t) == [{"id": "s", "v": 1}]
    log.info("判定依据: 两表均保持版本 %s，无部分生效",
             {t: ok.snapshots[t].snapshot_id for t in TABLES})

    # 同一 txn_id 重放：返回相同的拒绝原因，不产生新版本。
    with pytest.raises(TransactionConflictError) as exc2:
        catalog.commit_tables(_build_txn("txn-bad", _current_bases(catalog), "x"))
    assert exc2.value.detail == e.detail
    log.info("拒绝幂等重放: txn_id=txn-bad 原因与首次完全一致")


def test_concurrent_transactions_exactly_one_wins(catalog: Catalog):
    """N 个线程对同一批表并发提交事务：恰好一个整笔生效，其余整笔拒绝。"""
    seed = catalog.commit_tables(_build_txn("txn-seed", _current_bases(catalog), "s"))
    _log_result("txn-seed", seed)
    n = 6
    results: dict[str, object] = {}
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        txn_id = f"txn-race-{i}"
        # 所有线程都基于 seed 之后的同一组基版本。
        txn = _build_txn(txn_id, _current_bases(catalog), f"r{i}")
        barrier.wait()
        try:
            r = catalog.commit_tables(txn)
            results[txn_id] = ("committed",
                               {t: s.snapshot_id for t, s in r.snapshots.items()})
        except TransactionConflictError as e:
            results[txn_id] = ("rejected",
                               {t: f["reason"] for t, f in e.detail["tables"].items()})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    committed = {k: v for k, v in results.items() if v[0] == "committed"}
    rejected = {k: v for k, v in results.items() if v[0] == "rejected"}
    for txn_id, r in sorted(results.items()):
        log.info("并发事务结果: txn_id=%s -> %s", txn_id, r)
    assert len(committed) == 1, "并发事务必须恰好一个整笔生效"
    assert len(rejected) == n - 1
    # 失败者每张表的原因都是 commit_conflict（基版本过期）。
    assert all(set(reasons.values()) == {"commit_conflict"}
               for _, reasons in rejected.values())

    # 无部分生效：两张表的当前版本都等于胜者事务记录的版本。
    winner_versions = next(iter(committed.values()))[1]
    for t in TABLES:
        current = catalog.current_snapshot(t)
        log.info("判定依据: table=%s current=%s 胜者记录=%s",
                 t, current.snapshot_id, winner_versions[t])
        assert current.snapshot_id == winner_versions[t]
        # 数据不丢不覆盖：种子行 + 胜者行，恰好 2 行。
        assert len(catalog.read_rows(t)) == 2


def test_same_txn_id_concurrent_replay_same_result(catalog: Catalog):
    """同一 txn_id 并发重复提交：所有线程拿到完全一致的结果，只产生一组新版本。"""
    n = 6
    results: list[object] = []
    barrier = threading.Barrier(n)

    def worker() -> None:
        txn = _build_txn("txn-dup", _current_bases(catalog), "d")
        barrier.wait()
        results.append(catalog.commit_tables(txn))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    versions = [{t: s.snapshot_id for t, s in r.snapshots.items()} for r in results]
    log.info("幂等并发重放: txn_id=txn-dup 各线程结果=%s", versions)
    assert all(v == versions[0] for v in versions), "重放必须返回与首次完全一致的结果"
    # 只产生了一个新版本。
    for t in TABLES:
        assert catalog.current_snapshot(t).snapshot_id == versions[0][t]
        assert len(catalog.read_rows(t)) == 1


def test_table_order_does_not_change_result(catalog: Catalog):
    """事务里表的书写顺序不同，最终结果一致。"""
    bases = _current_bases(catalog)
    txn1 = _build_txn("txn-ord", bases, "o", order=("detail", "summary"))
    r1 = catalog.commit_tables(txn1)
    _log_result("txn-ord", r1)

    # 同一 txn_id、相反书写顺序重放：结果完全一致。
    txn2 = _build_txn("txn-ord", bases, "o", order=("summary", "detail"))
    r2 = catalog.commit_tables(txn2)
    v1 = {t: s.snapshot_id for t, s in r1.snapshots.items()}
    v2 = {t: s.snapshot_id for t, s in r2.snapshots.items()}
    log.info("书写顺序无关性: 顺序(detail,summary)=%s 顺序(summary,detail)=%s", v1, v2)
    assert v1 == v2


def test_read_transaction_failures_are_distinguishable(catalog: Catalog):
    """事务读取的拒绝与普通读取区分：未参与 / 不存在 / 版本已回收。"""
    r = catalog.commit_tables(_build_txn("txn-r", _current_bases(catalog), "r"))
    _log_result("txn-r", r)

    # 事务不存在。
    with pytest.raises(TransactionNotFoundError) as e1:
        catalog.transaction_snapshot("no-such-txn", "detail")
    log.info("事务不存在: reason=%s", e1.value.reason)
    assert e1.value.reason == "transaction_not_found"

    # 表未参与该事务。
    catalog.create_table("other", SCHEMA)
    with pytest.raises(TransactionNotFoundError) as e2:
        catalog.transaction_snapshot("txn-r", "other")
    log.info("表未参与: reason=%s detail=%s", e2.value.reason, e2.value.detail)
    assert e2.value.reason == "transaction_not_found"
    assert e2.value.detail["tables"] == sorted(TABLES)

    # 被拒绝的事务没有可读取的快照。
    with pytest.raises(TransactionConflictError):
        catalog.commit_tables(_build_txn("txn-rej", {"detail": None, "summary": None}, "z"))
    with pytest.raises(TransactionNotFoundError) as e3:
        catalog.transaction_snapshot("txn-rej", "detail")
    log.info("被拒绝事务读取: reason=%s txn_status=%s",
             e3.value.reason, e3.value.detail.get("txn_status"))
    assert e3.value.reason == "transaction_not_found"

    # 版本已回收：继续提交把 txn-r 的版本挤出租约窗口并清理。
    bases = _current_bases(catalog)
    for i in range(3):
        catalog.commit_tables(_build_txn(f"txn-fill-{i}", bases, f"f{i}"))
        bases = _current_bases(catalog)
    removed = {t: catalog.cleanup(t) for t in TABLES}
    log.info("清理: 回收=%s txn-r 版本=%s",
             removed, {t: s.snapshot_id for t, s in r.snapshots.items()})
    assert r.snapshots["detail"].snapshot_id in removed["detail"]

    with pytest.raises(TransactionSnapshotUnavailableError) as e4:
        catalog.transaction_snapshot("txn-r", "detail")
    log.info("版本已回收: reason=%s detail=%s", e4.value.reason, e4.value.detail)
    assert e4.value.reason == "transaction_snapshot_unavailable"
    # 与普通 snapshot_not_found 是可区分的不同 reason。
    assert e4.value.reason != SnapshotNotFoundError.reason
