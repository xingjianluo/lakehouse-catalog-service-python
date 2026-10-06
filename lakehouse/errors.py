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


# ---------- 跨表事务 ----------

class TxnConflictError(CatalogError):
    """跨表事务被拒绝：至少一张表基版本过期或结构不兼容。

    detail["failures"] 逐表给出 {reason, detail}，便于定位是哪张表、为什么失败。
    """

    reason = "txn_conflict"


class TxnNotFoundError(CatalogError):
    """按事务标识读取时，该事务从未提交过。"""

    reason = "txn_not_found"


class TxnNotCommittedError(CatalogError):
    """事务存在但被整体拒绝，没有可读取的快照组。"""

    reason = "txn_not_committed"


class TxnTableNotInvolvedError(CatalogError):
    """请求的表没有参与这笔事务。"""

    reason = "txn_table_not_involved"


class TxnSnapshotUnavailableError(CatalogError):
    """事务产生的快照已被清理回收，按事务读取不到当时的版本。"""

    reason = "txn_snapshot_unavailable"
