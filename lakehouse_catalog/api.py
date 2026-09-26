"""FastAPI HTTP entry point.

The HTTP layer is intentionally thin: every route validates nothing about
snapshots or schemas itself and delegates straight to
:class:`~lakehouse_catalog.catalog.LakehouseCatalog`, so the conclusions
(conflict vs stale base, expired vs unknown snapshot, incompatible
evolution reasons) are identical to using the catalog from Python.

``CatalogError`` subclasses are translated to their declared HTTP status
codes with a stable JSON body::

    {"error": "<code>", "message": "...", ...reason details...}
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import Body, FastAPI, Query
from fastapi.responses import JSONResponse

from .catalog import LakehouseCatalog
from .errors import (
    CatalogError,
    DuplicateCommitError,
)

log = logging.getLogger("lakehouse_catalog.api")


def create_app(warehouse_root: str | Path, *,
               require_explicit_base: bool = True) -> FastAPI:
    catalog = LakehouseCatalog(
        warehouse_root, require_explicit_base=require_explicit_base)
    app = FastAPI(
        title="Lakehouse Catalog Service",
        version="0.1.0",
        description="Local versioned table catalog: OCC commits, snapshot "
                    "isolation, history reads and schema evolution.",
    )

    @app.exception_handler(CatalogError)
    async def handle_catalog_error(request, exc: CatalogError) -> JSONResponse:  # type: ignore[no-untyped-def]
        # Duplicate commits are a successful (idempotent) outcome.
        status = 200 if isinstance(exc, DuplicateCommitError) else exc.http_status
        if status >= 500:  # pragma: no cover - no 5xx defined currently
            log.error("catalog failure: %s", exc.message)
        return JSONResponse(status_code=status, content=exc.to_dict())

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "warehouse": str(catalog.storage.root)}

    # ---- DDL --------------------------------------------------------
    @app.post("/tables")
    def create_table(payload: dict = Body(...)) -> dict[str, object]:  # type: ignore[no-untyped-def]
        return catalog.create_table(
            str(payload["name"]),
            list(payload.get("columns", [])),
            partition_by=payload.get("partition_by"),
            commit_id=payload.get("commit_id"),
        )

    @app.get("/tables")
    def list_tables() -> dict[str, object]:
        return {"tables": catalog.list_tables()}

    @app.get("/tables/{table}")
    def describe_table(table: str) -> dict[str, object]:
        return catalog.describe_table(table)

    @app.delete("/tables/{table}")
    def drop_table(table: str) -> dict[str, object]:
        catalog.drop_table(table)
        return {"table": table, "dropped": True}

    # ---- commits ----------------------------------------------------
    @app.post("/tables/{table}/commit")
    def commit(table: str, payload: dict = Body(...),  # type: ignore[no-untyped-def]
               commit_id: str | None = Query(default=None)) -> dict[str, object]:
        cid = payload.get("commit_id") or commit_id
        return catalog.commit(table, payload, commit_id=cid)

    # ---- reads ------------------------------------------------------
    @app.get("/tables/{table}/data")
    def read_table(
        table: str,
        version: int | None = Query(default=None),
        commit_id: str | None = Query(default=None),
        partition: str | None = Query(
            default=None,
            description="k=v pairs joined by ';', e.g. dt=d1;region=r2"),
    ) -> dict[str, object]:
        partitions = _parse_partitions(partition)
        return catalog.read_table(
            table, version=version, commit_id=commit_id,
            partitions=partitions)

    # ---- snapshots --------------------------------------------------
    @app.get("/tables/{table}/snapshots")
    def list_snapshots(table: str) -> dict[str, object]:
        return catalog.list_snapshots(table)

    @app.get("/tables/{table}/snapshots/{ref}")
    def get_snapshot(table: str, ref: str,
                     by: str = Query(default="version")) -> dict[str, object]:
        if by == "commit":
            return catalog.get_snapshot(table, commit_id=ref)
        if by != "version":
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error",
                         "message": "by must be 'version' or 'commit'"})
        try:
            version = int(ref)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error",
                         "message": f"snapshot ref {ref!r} is not an integer"})
        return catalog.get_snapshot(table, version=version)

    @app.post("/tables/{table}/snapshots/{version}/tag")
    def tag_snapshot(table: str, version: int) -> dict[str, object]:
        return catalog.tag_snapshot(table, version)

    # ---- retention --------------------------------------------------
    @app.post("/tables/{table}/expire")
    def expire(table: str, payload: dict | None = Body(default=None)) -> dict[str, object]:  # type: ignore[no-untyped-def]
        return catalog.expire_snapshots(table, payload)

    return app


def _parse_partitions(raw: str | None) -> dict[str, str] | None:
    if not raw:
        return None
    out: dict[str, str] = {}
    for pair in raw.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            from .errors import ValidationError
            raise ValidationError(
                f"partition filter {pair!r} must look like key=value",
                reason="malformed_partition_filter")
        k, v = pair.split("=", 1)
        out[k.strip()] = v.strip()
    return out or None
