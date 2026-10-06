"""目录服务核心：版本化提交、快照隔离、历史读取与清理。

磁盘布局（root 下每张表一个目录）：

    <table>/table.json              表指针：current_snapshot_id / next_snapshot_id（原子替换）
    <table>/snapshots/<id>.json     不可变快照元数据（含 schema 与文件清单）
    <table>/commits/<commit_id>.json 提交记录（幂等键 + 审计）
    <table>/pins.json               快照引用计数（pin）
    <table>/data/<commit_id>/...    分区数据文件（jsonl）

提交协议（乐观并发控制）：
1. begin_commit 记录基版本 base_snapshot_id；
2. commit 时在表级锁内校验 base == current，不等则整体拒绝（commit_conflict）；
3. 结构演进在提交点一次性判定，通过后才写任何表指针，并发提交不会读到中间态；
4. 数据文件与快照元数据先落盘，最后原子替换 table.json —— 整体生效或整体失败；
5. commit_id 为幂等键：相同 commit_id 的重试直接返回首次提交的结果。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import (
    CatalogError,
    CommitConflictError,
    SnapshotNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
)
from .models import Column, CommitRecord, PartitionFile, Schema, Snapshot
from .schema import backfill_row, check_compatible
from .storage import LocalStorage


# ---------- 序列化 ----------

def _schema_to_dict(s: Schema) -> dict:
    return {
        "columns": [
            {"name": c.name, "type": c.type, "default": c.default, "has_default": c.has_default}
            for c in s.columns
        ]
    }


def _schema_from_dict(d: dict) -> Schema:
    return Schema(tuple(
        Column(c["name"], c["type"], c.get("default"), c.get("has_default", False))
        for c in d["columns"]
    ))


def _file_to_dict(f: PartitionFile) -> dict:
    return {"path": f.path, "partition": f.partition, "row_count": f.row_count}


def _file_from_dict(d: dict) -> PartitionFile:
    return PartitionFile(d["path"], dict(d["partition"]), d["row_count"])


def _snapshot_to_dict(s: Snapshot) -> dict:
    return {
        "snapshot_id": s.snapshot_id,
        "commit_id": s.commit_id,
        "parent_snapshot_id": s.parent_snapshot_id,
        "schema": _schema_to_dict(s.schema),
        "files": [_file_to_dict(f) for f in s.files],
        "created_at": s.created_at,
        "summary": s.summary,
    }


def _snapshot_from_dict(d: dict) -> Snapshot:
    return Snapshot(
        snapshot_id=d["snapshot_id"],
        commit_id=d["commit_id"],
        parent_snapshot_id=d["parent_snapshot_id"],
        schema=_schema_from_dict(d["schema"]),
        files=tuple(_file_from_dict(f) for f in d["files"]),
        created_at=d["created_at"],
        summary=dict(d.get("summary", {})),
    )


# ---------- 事务句柄 ----------

@dataclass
class Transaction:
    """一次未决提交：记录基版本并暂存全部变更。"""

    table: str
    base_snapshot_id: int | None
    adds: list[tuple[dict[str, str], list[dict]]] = field(default_factory=list)
    removes: list[str] = field(default_factory=list)
    new_schema: Schema | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    finished: bool = False

    def add_partition(self, partition: dict[str, str], rows: list[dict]) -> "Transaction":
        self.adds.append((dict(partition), list(rows)))
        return self

    def remove_file(self, path: str) -> "Transaction":
        self.removes.append(path)
        return self

    def set_schema(self, schema: Schema) -> "Transaction":
        self.new_schema = schema
        return self


# ---------- 目录 ----------

class Catalog:
    def __init__(self, root: Path, *, retain_last: int = 3) -> None:
        self.storage = LocalStorage(Path(root))
        self.retain_last = retain_last
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---- 路径 ----
    def _table_dir(self, table: str) -> Path:
        return Path(table)

    def _table_meta_path(self, table: str) -> Path:
        return self._table_dir(table) / "table.json"

    def _snapshot_path(self, table: str, snapshot_id: int) -> Path:
        return self._table_dir(table) / "snapshots" / f"{snapshot_id}.json"

    def _commit_path(self, table: str, commit_id: str) -> Path:
        return self._table_dir(table) / "commits" / f"{commit_id}.json"

    def _pins_path(self, table: str) -> Path:
        return self._table_dir(table) / "pins.json"

    def _lock_for(self, table: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(table, threading.Lock())

    # ---- 表管理 ----
    def create_table(self, name: str, schema: Schema) -> None:
        lock = self._lock_for(name)
        with lock:
            if self.storage.exists(self._table_meta_path(name)):
                raise TableAlreadyExistsError(f"表 {name!r} 已存在", detail={"table": name})
            self.storage.write_json_atomic(
                self._table_meta_path(name),
                {"name": name, "current_snapshot_id": None, "next_snapshot_id": 1,
                 "schema": _schema_to_dict(schema)},
            )

    def _load_table_meta(self, table: str) -> dict:
        path = self._table_meta_path(table)
        if not self.storage.exists(path):
            raise TableNotFoundError(f"表 {table!r} 不存在", detail={"table": table})
        return self.storage.read_json(path)

    # ---- 提交协议 ----
    def begin_commit(self, table: str) -> Transaction:
        meta = self._load_table_meta(table)
        return Transaction(table=table, base_snapshot_id=meta["current_snapshot_id"])

    def _validate_change(self, tx: Transaction, meta: dict) -> None:
        """校验一笔表变更：基版本冲突检测 + 结构演进判定。失败抛 CatalogError。"""
        current_id = meta["current_snapshot_id"]
        if tx.base_snapshot_id != current_id:
            raise CommitConflictError(
                f"基于过期快照 {tx.base_snapshot_id} 提交，当前版本为 {current_id}",
                detail={"expected_base": current_id, "actual_base": tx.base_snapshot_id},
            )
        if tx.new_schema is not None:
            current_schema = (
                self.get_snapshot(tx.table, current_id).schema
                if current_id is not None
                else _schema_from_dict(meta["schema"])
            )
            check_compatible(current_schema, tx.new_schema)

    def _stage_change(
        self, tx: Transaction, meta: dict, commit_id: str
    ) -> tuple[Snapshot, CommitRecord]:
        """暂存数据文件并组装新快照与提交记录（尚未落盘快照/指针）。"""
        current_id = meta["current_snapshot_id"]
        current_schema = (
            self.get_snapshot(tx.table, current_id).schema
            if current_id is not None
            else _schema_from_dict(meta["schema"])
        )
        new_schema = tx.new_schema or current_schema

        # 暂存数据文件（按 commit_id 隔离，失败不影响现有快照）。
        new_files: list[PartitionFile] = []
        for i, (partition, rows) in enumerate(tx.adds):
            rel = self._table_dir(tx.table) / "data" / commit_id / f"part-{i:05d}.jsonl"
            self.storage.write_data_file(rel, rows)
            new_files.append(PartitionFile(str(rel), partition, len(rows)))

        # 组装新快照：继承现有文件，应用删除与新增。
        existing: list[PartitionFile] = []
        if current_id is not None:
            existing = list(self.get_snapshot(tx.table, current_id).files)
        removed = set(tx.removes)
        files = tuple(f for f in existing if f.path not in removed) + tuple(new_files)

        snapshot = Snapshot(
            snapshot_id=meta["next_snapshot_id"],
            commit_id=commit_id,
            parent_snapshot_id=current_id,
            schema=new_schema,
            files=files,
            created_at=time.time(),
            summary=dict(tx.summary),
        )
        record = CommitRecord(
            commit_id=commit_id, table=tx.table,
            base_snapshot_id=tx.base_snapshot_id, snapshot_id=snapshot.snapshot_id,
            status="committed", reason=None,
            detail={"files_added": len(new_files), "files_removed": len(removed)},
        )
        return snapshot, record

    def _persist_change(
        self, table: str, meta: dict, snapshot: Snapshot, record: CommitRecord
    ) -> None:
        """先写快照与提交记录，最后原子切换表指针：整体生效或整体失败。"""
        self.storage.write_json_atomic(
            self._snapshot_path(table, snapshot.snapshot_id), _snapshot_to_dict(snapshot))
        self.storage.write_json_atomic(self._commit_path(table, record.commit_id),
                                       _record_to_dict(record))
        meta["current_snapshot_id"] = snapshot.snapshot_id
        meta["next_snapshot_id"] = snapshot.snapshot_id + 1
        self.storage.write_json_atomic(self._table_meta_path(table), meta)

    def commit(self, tx: Transaction, *, commit_id: str | None = None) -> Snapshot:
        if tx.finished:
            raise CatalogError("事务已结束，不能重复提交", detail={"table": tx.table})
        commit_id = commit_id or uuid.uuid4().hex
        lock = self._lock_for(tx.table)
        with lock:
            # 幂等：同一 commit_id 已成功提交过，直接返回首次结果。
            commit_path = self._commit_path(tx.table, commit_id)
            if self.storage.exists(commit_path):
                record = self.storage.read_json(commit_path)
                tx.finished = True
                if record["status"] == "committed":
                    return self.get_snapshot(tx.table, record["snapshot_id"])
                raise CommitConflictError(
                    f"commit {commit_id} 此前已被拒绝: {record['reason']}",
                    detail=record.get("detail", {}),
                )

            meta = self._load_table_meta(tx.table)

            # 冲突检测与结构演进判定：失败则整体拒绝，并记录审计。
            try:
                self._validate_change(tx, meta)
            except CatalogError as e:
                record = CommitRecord(
                    commit_id=commit_id, table=tx.table,
                    base_snapshot_id=tx.base_snapshot_id, snapshot_id=None,
                    status="rejected", reason=e.reason, detail=e.detail,
                )
                self.storage.write_json_atomic(commit_path, _record_to_dict(record))
                tx.finished = True
                raise

            snapshot, record = self._stage_change(tx, meta, commit_id)
            self._persist_change(tx.table, meta, snapshot, record)
            tx.finished = True
            return snapshot

    # ---- 读取 ----
    def current_snapshot(self, table: str) -> Snapshot:
        meta = self._load_table_meta(table)
        if meta["current_snapshot_id"] is None:
            raise SnapshotNotFoundError(
                f"表 {table!r} 尚无任何提交", detail={"table": table})
        return self.get_snapshot(table, meta["current_snapshot_id"])

    def get_snapshot(self, table: str, snapshot_id: int) -> Snapshot:
        self._load_table_meta(table)
        path = self._snapshot_path(table, snapshot_id)
        if not self.storage.exists(path):
            raise SnapshotNotFoundError(
                f"快照 {snapshot_id} 不存在或已被回收",
                detail={"table": table, "snapshot_id": snapshot_id},
            )
        return _snapshot_from_dict(self.storage.read_json(path))

    def get_snapshot_by_commit(self, table: str, commit_id: str) -> Snapshot:
        self._load_table_meta(table)
        path = self._commit_path(table, commit_id)
        if not self.storage.exists(path):
            raise SnapshotNotFoundError(
                f"提交 {commit_id!r} 不存在",
                detail={"table": table, "commit_id": commit_id},
            )
        record = self.storage.read_json(path)
        if record["status"] != "committed":
            raise SnapshotNotFoundError(
                f"提交 {commit_id!r} 未成功（{record['reason']}），无对应快照",
                detail={"table": table, "commit_id": commit_id, "reason": record["reason"]},
            )
        return self.get_snapshot(table, record["snapshot_id"])

    def read_rows(self, table: str, snapshot_id: int | None = None) -> list[dict]:
        snap = self.current_snapshot(table) if snapshot_id is None else self.get_snapshot(table, snapshot_id)
        rows: list[dict] = []
        for f in snap.files:
            for row in self.storage.read_data_file(Path(f.path)):
                rows.append(backfill_row(row, snap.schema))
        return rows

    def list_partitions(self, table: str, snapshot_id: int | None = None) -> list[dict]:
        snap = self.current_snapshot(table) if snapshot_id is None else self.get_snapshot(table, snapshot_id)
        return [
            {"partition": dict(f.partition), "path": f.path, "row_count": f.row_count}
            for f in snap.files
        ]

    # ---- 引用与清理 ----
    def _load_pins(self, table: str) -> dict[str, int]:
        path = self._pins_path(table)
        if not self.storage.exists(path):
            return {}
        return {str(k): int(v) for k, v in self.storage.read_json(path).items()}

    def pin_snapshot(self, table: str, snapshot_id: int) -> None:
        lock = self._lock_for(table)
        with lock:
            self.get_snapshot(table, snapshot_id)  # 不存在则拒绝
            pins = self._load_pins(table)
            pins[str(snapshot_id)] = pins.get(str(snapshot_id), 0) + 1
            self.storage.write_json_atomic(self._pins_path(table), pins)

    def unpin_snapshot(self, table: str, snapshot_id: int) -> None:
        lock = self._lock_for(table)
        with lock:
            pins = self._load_pins(table)
            key = str(snapshot_id)
            if pins.get(key, 0) > 1:
                pins[key] -= 1
            else:
                pins.pop(key, None)
            self.storage.write_json_atomic(self._pins_path(table), pins)

    def cleanup(self, table: str) -> list[int]:
        """回收未被 pin 且不在最新 retain_last 个快照内的历史快照。

        同时删除不再被任何保留快照引用的数据文件。
        被 pin 的快照及其数据文件绝不回收。
        """
        lock = self._lock_for(table)
        with lock:
            meta = self._load_table_meta(table)
            snap_dir = self._table_dir(table) / "snapshots"
            abs_dir = self.storage.root / snap_dir
            if not abs_dir.exists():
                return []
            all_ids = sorted(int(p.stem) for p in abs_dir.glob("*.json"))
            pins = self._load_pins(table)
            current_id = meta["current_snapshot_id"]

            keep_recent = set(all_ids[-self.retain_last:]) if self.retain_last > 0 else set()
            keep_recent.add(current_id)
            removed: list[int] = []
            kept_ids: list[int] = []
            for sid in all_ids:
                if sid in keep_recent or pins.get(str(sid), 0) > 0:
                    kept_ids.append(sid)
                else:
                    removed.append(sid)

            # 计算保留快照仍引用的数据文件。
            referenced: set[str] = set()
            for sid in kept_ids:
                snap = self.get_snapshot(table, sid)
                referenced.update(f.path for f in snap.files)

            for sid in removed:
                snap = _snapshot_from_dict(
                    self.storage.read_json(self._snapshot_path(table, sid)))
                for f in snap.files:
                    if f.path not in referenced:
                        self.storage.delete(Path(f.path))
                self.storage.delete(self._snapshot_path(table, sid))
            return removed

    def commit_log(self, table: str) -> list[dict]:
        self._load_table_meta(table)
        abs_dir = self.storage.root / self._table_dir(table) / "commits"
        if not abs_dir.exists():
            return []
        records = [self.storage.read_json(p) for p in sorted(abs_dir.glob("*.json"))]
        return sorted(records, key=lambda r: (r["snapshot_id"] is None, r["snapshot_id"] or 0, r["commit_id"]))


def _record_to_dict(r: CommitRecord) -> dict:
    return {
        "commit_id": r.commit_id,
        "table": r.table,
        "base_snapshot_id": r.base_snapshot_id,
        "snapshot_id": r.snapshot_id,
        "status": r.status,
        "reason": r.reason,
        "detail": r.detail,
    }
