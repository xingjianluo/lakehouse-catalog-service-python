"""FastAPI 入口：本地湖仓表目录服务。

错误映射：所有 CatalogError 返回 JSON {"reason": ..., "message": ..., "detail": ...}，
HTTP 状态码按类别区分：
- 404 表/快照不存在
- 409 提交冲突 / 表已存在
- 422 结构演进不兼容
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from lakehouse.catalog import Catalog
from lakehouse.errors import (
    CatalogError,
    CommitConflictError,
    SchemaEvolutionError,
    SnapshotNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
    TxnConflictError,
    TxnNotCommittedError,
    TxnNotFoundError,
    TxnSnapshotUnavailableError,
    TxnTableNotInvolvedError,
)
from lakehouse.models import Column, Schema

CATALOG_ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", tempfile.mkdtemp(prefix="lakehouse-")))
catalog = Catalog(CATALOG_ROOT)
app = FastAPI(title="lakehouse-catalog")


@app.exception_handler(CatalogError)
async def catalog_error_handler(_: Request, exc: CatalogError) -> JSONResponse:
    status = 400
    if isinstance(exc, (TableNotFoundError, SnapshotNotFoundError,
                       TxnNotFoundError, TxnTableNotInvolvedError)):
        status = 404
    elif isinstance(exc, (CommitConflictError, TableAlreadyExistsError,
                          TxnConflictError, TxnNotCommittedError)):
        status = 409
    elif isinstance(exc, TxnSnapshotUnavailableError):
        status = 410
    elif isinstance(exc, SchemaEvolutionError):
        status = 422
    return JSONResponse(
        status_code=status,
        content={"reason": exc.reason, "message": exc.message, "detail": exc.detail},
    )


# ---------- 请求模型 ----------

class ColumnIn(BaseModel):
    name: str
    type: str
    default: Any = None
    has_default: bool = False


class CreateTableIn(BaseModel):
    table: str
    columns: list[ColumnIn]


class PartitionAddIn(BaseModel):
    partition: dict[str, str]
    rows: list[dict[str, Any]]


class CommitIn(BaseModel):
    commit_id: str | None = None
    # 必填且可空：客户端必须显式声明基版本，null 表示"建表后尚无提交"。
    base_snapshot_id: int | None
    adds: list[PartitionAddIn] = []
    removes: list[str] = []
    new_columns: list[ColumnIn] | None = None  # 完整新结构（提供则触发演进判定）
    summary: dict[str, Any] = {}


class TxnTableIn(BaseModel):
    """跨表事务中一张表的变更，字段语义与单表 CommitIn 一致。"""

    table: str
    base_snapshot_id: int | None
    adds: list[PartitionAddIn] = []
    removes: list[str] = []
    new_columns: list[ColumnIn] | None = None
    summary: dict[str, Any] = {}


class TxnCommitIn(BaseModel):
    txn_id: str | None = None
    tables: list[TxnTableIn]


def _to_schema(cols: list[ColumnIn]) -> Schema:
    return Schema(tuple(Column(c.name, c.type, c.default, c.has_default) for c in cols))


def _snapshot_out(s) -> dict:
    return {
        "snapshot_id": s.snapshot_id,
        "commit_id": s.commit_id,
        "parent_snapshot_id": s.parent_snapshot_id,
        "schema": {"columns": [
            {"name": c.name, "type": c.type, "default": c.default, "has_default": c.has_default}
            for c in s.schema.columns
        ]},
        "files": [{"path": f.path, "partition": f.partition, "row_count": f.row_count}
                  for f in s.files],
        "created_at": s.created_at,
        "summary": s.summary,
    }


# ---------- 路由 ----------

@app.get("/")
def read_root():
    return {"service": "lakehouse-catalog", "status": "ok"}


@app.post("/tables", status_code=201)
def create_table(body: CreateTableIn):
    catalog.create_table(body.table, _to_schema(body.columns))
    return {"table": body.table, "created": True}


@app.post("/tables/{table}/commits", status_code=201)
def commit(table: str, body: CommitIn):
    """单次请求完成 begin+commit：客户端声明 base_snapshot_id 参与乐观并发控制。"""
    tx = catalog.begin_commit(table)
    # 必填字段：客户端读到的基版本（含 null）原样参与冲突检测。
    tx.base_snapshot_id = body.base_snapshot_id
    for add in body.adds:
        tx.add_partition(add.partition, add.rows)
    for path in body.removes:
        tx.remove_file(path)
    if body.new_columns is not None:
        tx.set_schema(_to_schema(body.new_columns))
    tx.summary.update(body.summary)
    snap = catalog.commit(tx, commit_id=body.commit_id)
    return _snapshot_out(snap)


@app.get("/tables/{table}/snapshot")
def current_snapshot(table: str):
    return _snapshot_out(catalog.current_snapshot(table))


@app.get("/tables/{table}/snapshots/{snapshot_id}")
def get_snapshot(table: str, snapshot_id: int):
    return _snapshot_out(catalog.get_snapshot(table, snapshot_id))


@app.get("/tables/{table}/commits/{commit_id}")
def get_by_commit(table: str, commit_id: str):
    return _snapshot_out(catalog.get_snapshot_by_commit(table, commit_id))


@app.get("/tables/{table}/rows")
def read_rows(table: str, snapshot_id: int | None = None):
    return {"rows": catalog.read_rows(table, snapshot_id)}


@app.get("/tables/{table}/partitions")
def list_partitions(table: str, snapshot_id: int | None = None):
    return {"partitions": catalog.list_partitions(table, snapshot_id)}


@app.post("/tables/{table}/pins/{snapshot_id}")
def pin(table: str, snapshot_id: int):
    catalog.pin_snapshot(table, snapshot_id)
    return {"pinned": snapshot_id}


@app.delete("/tables/{table}/pins/{snapshot_id}")
def unpin(table: str, snapshot_id: int):
    catalog.unpin_snapshot(table, snapshot_id)
    return {"unpinned": snapshot_id}


@app.post("/tables/{table}/cleanup")
def cleanup(table: str):
    return {"removed": catalog.cleanup(table)}


@app.get("/tables/{table}/commits")
def commit_log(table: str):
    return {"commits": catalog.commit_log(table)}


# ---------- 跨表事务 ----------

@app.post("/txns", status_code=201)
def commit_txn(body: TxnCommitIn):
    """跨表原子提交：全部表生效或全部拒绝，txn_id 为幂等键。"""
    txn = catalog.begin_txn([t.table for t in body.tables])
    for t in body.tables:
        change = txn.change(t.table)
        change.base_snapshot_id = t.base_snapshot_id
        for add in t.adds:
            change.add_partition(add.partition, add.rows)
        for path in t.removes:
            change.remove_file(path)
        if t.new_columns is not None:
            change.set_schema(_to_schema(t.new_columns))
        change.summary.update(t.summary)
    snaps = catalog.commit_txn(txn, txn_id=body.txn_id)
    txn_id = next(iter(snaps.values())).commit_id
    return {"txn_id": txn_id,
            "snapshots": {t: _snapshot_out(s) for t, s in snaps.items()}}


@app.get("/txns/{txn_id}")
def get_txn(txn_id: str):
    """按事务标识读取整组自洽快照。"""
    snaps = catalog.get_txn_snapshots(txn_id)
    return {"txn_id": txn_id,
            "snapshots": {t: _snapshot_out(s) for t, s in snaps.items()}}


@app.get("/txns/{txn_id}/tables/{table}/rows")
def get_txn_rows(txn_id: str, table: str):
    """按事务标识读取其中一张表当时的行。"""
    snap = catalog.get_txn_snapshot(txn_id, table)
    return {"rows": catalog.read_rows(table, snap.snapshot_id)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
