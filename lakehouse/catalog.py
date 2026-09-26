"""目录服务核心：版本化提交、快照隔离、历史读取与清理（骨架）。"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from .models import Schema, Snapshot
from .storage import LocalStorage


class Catalog:
    def __init__(self, root: Path, *, retain_last: int = 3) -> None:
        self.storage = LocalStorage(Path(root))
        self.retain_last = retain_last
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---- 表管理 ----
    def create_table(self, name: str, schema: Schema) -> None:
        raise NotImplementedError

    # ---- 提交协议 ----
    def begin_commit(self, table: str) -> Any:
        """返回一个事务句柄，记录基版本并暂存变更。"""
        raise NotImplementedError

    def commit(self, tx: Any, *, commit_id: str | None = None) -> Snapshot:
        """原子提交：整体生效或整体失败；过期基版本被拒绝。"""
        raise NotImplementedError

    # ---- 读取 ----
    def current_snapshot(self, table: str) -> Snapshot:
        raise NotImplementedError

    def get_snapshot(self, table: str, snapshot_id: int) -> Snapshot:
        raise NotImplementedError

    def get_snapshot_by_commit(self, table: str, commit_id: str) -> Snapshot:
        raise NotImplementedError

    def read_rows(self, table: str, snapshot_id: int | None = None) -> list[dict]:
        raise NotImplementedError

    def list_partitions(self, table: str, snapshot_id: int | None = None) -> list[dict]:
        raise NotImplementedError

    # ---- 引用与清理 ----
    def pin_snapshot(self, table: str, snapshot_id: int) -> None:
        raise NotImplementedError

    def unpin_snapshot(self, table: str, snapshot_id: int) -> None:
        raise NotImplementedError

    def cleanup(self, table: str) -> list[int]:
        """回收未被引用且超出保留窗口的历史快照，返回被回收的 id。"""
        raise NotImplementedError

    def commit_log(self, table: str) -> list[dict]:
        raise NotImplementedError
