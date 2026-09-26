"""表元数据、快照与提交记录的不可变数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# 允许的类型及其可安全提升方向（widening）。
TYPE_WIDENING: dict[str, set[str]] = {
    "int": {"long", "double"},
    "long": {"double"},
    "float": {"double"},
    "double": set(),
    "string": set(),
    "bool": set(),
}


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    default: Any = None
    has_default: bool = False


@dataclass(frozen=True)
class Schema:
    columns: tuple[Column, ...]

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


@dataclass(frozen=True)
class PartitionFile:
    """一个分区数据文件：本地路径 + 行数。"""

    path: str
    partition: dict[str, str]
    row_count: int


@dataclass(frozen=True)
class Snapshot:
    """一次提交产生的不可变快照。"""

    snapshot_id: int
    commit_id: str
    parent_snapshot_id: int | None
    schema: Schema
    files: tuple[PartitionFile, ...]
    created_at: float
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CommitRecord:
    """提交审计记录：谁、基于哪个版本、结果如何。"""

    commit_id: str
    table: str
    base_snapshot_id: int | None
    snapshot_id: int | None
    status: str  # "committed" | "rejected"
    reason: str | None
    detail: dict[str, Any] = field(default_factory=dict)
