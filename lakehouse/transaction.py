"""跨表事务的数据模型。

事务语义（实现在 catalog.Catalog 上）：
- txn_id 为幂等键，提交记录落盘在 <root>/_transactions/<txn_id>.json；
- 一笔事务覆盖多张表：全部生效或全部不生效；
- 按 txn_id 可读取该事务在所有参与表上产生的一组自洽快照。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import Schema, Snapshot


@dataclass
class TableChange:
    """事务中针对单张表的变更集。"""

    table: str
    base_snapshot_id: int | None
    adds: list[tuple[dict[str, str], list[dict]]] = field(default_factory=list)
    removes: list[str] = field(default_factory=list)
    new_schema: Schema | None = None
    summary: dict[str, Any] = field(default_factory=dict)

    def add_partition(self, partition: dict[str, str], rows: list[dict]) -> "TableChange":
        self.adds.append((dict(partition), list(rows)))
        return self

    def remove_file(self, path: str) -> "TableChange":
        self.removes.append(path)
        return self

    def set_schema(self, schema: Schema) -> "TableChange":
        self.new_schema = schema
        return self


@dataclass
class MultiTableTransaction:
    """一笔跨表事务：txn_id 为幂等键，changes 覆盖多张表（表名为键）。"""

    txn_id: str
    changes: dict[str, TableChange] = field(default_factory=dict)

    def table(self, name: str, base_snapshot_id: int | None) -> TableChange:
        change = TableChange(table=name, base_snapshot_id=base_snapshot_id)
        self.changes[name] = change
        return change


@dataclass(frozen=True)
class TransactionResult:
    """事务提交成功后的结果：每张参与表对应的快照（表名 -> Snapshot）。"""

    txn_id: str
    snapshots: dict[str, Snapshot]
