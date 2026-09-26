"""目录服务的可区分错误类型。

每个错误携带 machine-readable 的 reason 字符串，
便于调用方与测试按原因区分失败。
"""

from __future__ import annotations


class CatalogError(Exception):
    """所有目录错误的基类。"""

    reason: str = "catalog_error"

    def __init__(self, message: str, *, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


class TableNotFoundError(CatalogError):
    reason = "table_not_found"


class TableAlreadyExistsError(CatalogError):
    reason = "table_already_exists"


class SnapshotNotFoundError(CatalogError):
    """请求的快照不存在或已被回收。"""

    reason = "snapshot_not_found"


class CommitConflictError(CatalogError):
    """基于过期版本的提交被拒绝（乐观并发控制冲突）。"""

    reason = "commit_conflict"


class SchemaEvolutionError(CatalogError):
    """结构演进不兼容被拒绝，reason 进一步细分。"""

    reason = "schema_incompatible"


class SchemaNarrowingError(SchemaEvolutionError):
    reason = "schema_type_narrowing"


class SchemaColumnDroppedError(SchemaEvolutionError):
    reason = "schema_column_dropped"


class SchemaColumnRenamedError(SchemaEvolutionError):
    reason = "schema_column_renamed"


class SchemaDefaultMissingError(SchemaEvolutionError):
    reason = "schema_default_missing"
