"""Core catalog service.

Commit protocol (optimistic concurrency, strictly serialized per table):

1. Take the exclusive table lock; re-read metadata from disk, so the
   decision is always made against the newest durable state regardless of
   entry point (Python API or HTTP).
2. If ``commit_id`` already appears in the append-only ledger, return the
   original result (idempotent retry) -- the commit is never applied twice.
3. Compare ``base_version`` with the current version:
   * equal            -> proceed;
   * older & retained -> ``concurrent_commit_conflict`` (someone else
                         committed first; the caller may retry on the new
                         base);
   * older & reclaimed-> ``stale_base_version`` (the base is gone);
   * ahead/unknown    -> ``validation_error``.
4. Validate schema changes *before* touching any data; incompatible
   changes reject the whole commit.
5. Write all new data files and the immutable version record first, then
   atomically swap ``metadata.json``. Readers therefore observe either the
   old complete snapshot or the new one -- never a partial write.
6. Append the commit to the ledger and log commit id / version / reason.

Reads always resolve snapshot + schema + partition spec together, so a
historical read can never mix an old file list with a new schema.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from . import datatypes
from .errors import (
    ConcurrentCommitConflictError,
    DataFileNotFoundError,
    DuplicateCommitError,
    SnapshotExpiredError,
    SnapshotNotFoundError,
    StaleBaseError,
    TableAlreadyExistsError,
    TableNotFoundError,
    ValidationError,
)
from .ledger import CommitLedger, LedgerEntry
from .model import DataFile, Snapshot, TableMetadata
from .retention import RetentionPolicy, parse_ts, select_expired
from .schema import (
    Column,
    EvolutionPlan,
    Schema,
    apply_add,
    apply_widen,
    validate_evolution,
    validate_row,
)
from .storage import FileStorage

log = logging.getLogger("lakehouse_catalog")


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class LakehouseCatalog:
    def __init__(self, warehouse_root: str | Path, *,
                 require_explicit_base: bool = False) -> None:
        self.storage = FileStorage(warehouse_root)
        # When True, every non-create commit must state the snapshot version
        # it was built against ("optimistic locking"). The HTTP service sets
        # this so true concurrent writers conflict; direct Python users may
        # leave it False for convenient serialized appends.
        self.require_explicit_base = require_explicit_base

    # =================================================================
    # DDL
    # =================================================================
    def create_table(
        self,
        name: str,
        columns: list[dict[str, object]],
        *,
        partition_by: list[str] | None = None,
        commit_id: str | None = None,
    ) -> dict[str, object]:
        commit_id = _commit_id(commit_id)
        with self.storage.table_lock(name):
            exists = self.storage.table_exists(name)
            if exists:
                # Idempotent create with the same commit id.
                dup = self._find_ledger(name, commit_id)
                if dup is not None:
                    return self._commit_result(self._load(name), dup.version,
                                               idempotent=True,
                                               reason="commit_id_already_applied")
                raise TableAlreadyExistsError(
                    f"table {name!r} already exists", table=name)

            schema = self._build_schema(columns, version=1)
            spec = self._validate_partition_spec(partition_by, schema)
            self.storage.init_table_dirs(name)
            try:
                snapshot = Snapshot(
                    version=1, commit_id=commit_id, parent_version=None,
                    schema_version=1, file_ids=(), partition_spec=spec,
                    timestamp=_utcnow_iso(), operation="create_table",
                )
                meta = TableMetadata(
                    name=name, location=str(self.storage.table_dir(name)),
                    current_version=1, schemas={1: schema},
                    snapshots={1: snapshot}, partition_spec=spec,
                )
                self._write_version_record(name, snapshot, schema, [])
                self.storage.atomic_write_json(
                    self.storage.metadata_path(name), meta.to_dict())
                self._ledger(name).append(LedgerEntry(
                    commit_id=commit_id, version=1, status="applied",
                    parent_version=None, timestamp=snapshot.timestamp))
            except BaseException:
                # Roll back an incomplete table directory.
                self.storage.delete_table(name)
                raise
        log.info("commit accepted table=%s commit_id=%s version=1 "
                 "operation=create_table decision=accept reason=table_created",
                 name, commit_id)
        return self._commit_result(meta, 1)

    def drop_table(self, name: str) -> None:
        if not self.storage.table_exists(name):
            raise TableNotFoundError(f"table {name!r} not found", table=name)
        with self.storage.table_lock(name):
            if self.storage.table_exists(name):
                self.storage.delete_table(name)
        log.info("table dropped table=%s", name)

    def list_tables(self) -> list[str]:
        return self.storage.list_tables()

    def describe_table(self, name: str) -> dict[str, object]:
        meta = self._load(name)
        snap = meta.current_snapshot()
        return {
            "name": meta.name,
            "location": meta.location,
            "current_version": meta.current_version,
            "current_commit_id": snap.commit_id,
            "partition_spec": list(meta.partition_spec),
            "schema": meta.current_schema().to_dict(),
        }

    # =================================================================
    # Commit
    # =================================================================
    def commit(
        self,
        table: str,
        request: dict[str, object],
        *,
        commit_id: str | None = None,
    ) -> dict[str, object]:
        commit_id = _commit_id(commit_id)
        if not isinstance(request, dict):
            raise ValidationError("commit request must be an object")
        with self.storage.table_lock(table):
            meta = self._load(table)
            ledger = self._ledger(table)

            dup = self._find_ledger(table, commit_id)
            if dup is not None:
                log.info("commit duplicate table=%s commit_id=%s version=%s "
                         "decision=reject reason=commit_id_already_applied",
                         table, commit_id, dup.version)
                raise DuplicateCommitError(
                    f"commit {commit_id!r} was already applied as version "
                    f"{dup.version}",
                    commit_id=commit_id, version=dup.version,
                    table=table)

            current = meta.current_version
            if "base_version" in request:
                base_version = request["base_version"]
            else:
                if self.require_explicit_base:
                    raise ValidationError(
                        "commit requires explicit base_version (the snapshot "
                        "version the change was built against)",
                        reason="missing_base_version", current_version=current)
                base_version = current
            if not isinstance(base_version, int) or isinstance(base_version, bool):
                raise ValidationError("base_version must be an integer",
                                      field="base_version")
            base = self._check_base(table, meta, base_version, current)
            base_snapshot = meta.snapshots[base]
            schema = meta.schemas[base_snapshot.schema_version]

            plan: EvolutionPlan | None = None
            change = request.get("schema_change")
            if change is not None:
                if not isinstance(change, dict):
                    raise ValidationError("schema_change must be an object",
                                          field="schema_change")
                plan = validate_evolution(schema, change)

            append_groups = _extract_groups(request, "append")
            overwrite_groups = _extract_groups(request, "overwrite")
            if plan is None and not append_groups and not overwrite_groups:
                raise ValidationError(
                    "commit is empty: provide append/overwrite rows or a "
                    "schema_change",
                    reason="empty_commit")

            new_schema = schema
            if plan is not None:
                if plan.action == "add_column":
                    new_schema = apply_add(schema, plan.column)
                else:
                    new_schema = apply_widen(schema, plan.target_name or plan.column.name,
                                             plan.column)

            new_version = current + 1
            operation = _operation_name(plan, append_groups, overwrite_groups)

            # ---- build new data files --------------------------------
            added: list[DataFile] = []
            removed_ids: set[str] = set()
            if overwrite_groups:
                live_ids: list[str] = []
                removed_ids.update(base_snapshot.file_ids)
            else:
                live_ids = list(base_snapshot.file_ids)

            for groups in (overwrite_groups, append_groups):
                for group in groups:
                    df = self._build_data_file(
                        table, new_version, new_schema,
                        group, meta.partition_spec)
                    added.append(df)
                    live_ids.append(df.file_id)

            # ---- materialized backfill for add_column ----------------
            backfill_rewritten: list[DataFile] = []
            if plan is not None and plan.action == "add_column" \
                    and plan.backfill == "materialized":
                inherited = [fid for fid in live_ids
                             if fid not in {d.file_id for d in added}]
                rewritten_ids = set()
                for fid in inherited:
                    old_df = self._read_file(table, fid)
                    if old_df.schema_version >= new_schema.version:
                        continue
                    rows = tuple(
                        _project_row(r, new_schema) for r in old_df.rows)
                    new_id = f"v{new_version:06d}-{uuid.uuid4().hex[:12]}"
                    rw = DataFile(new_id, rows, new_schema.version,
                                  dict(old_df.partition_values))
                    backfill_rewritten.append(rw)
                    rewritten_ids.add(old_df.file_id)
                    live_ids[live_ids.index(fid)] = new_id
                for df in backfill_rewritten:
                    self.storage.write_data_file(table, df)
                removed_ids.update(rewritten_ids)

            for df in added:
                self.storage.write_data_file(table, df)

            added_ids = tuple(d.file_id for d in added) + \
                tuple(d.file_id for d in backfill_rewritten)

            snapshot = Snapshot(
                version=new_version, commit_id=commit_id,
                parent_version=current, schema_version=new_schema.version,
                file_ids=tuple(live_ids), partition_spec=meta.partition_spec,
                timestamp=_utcnow_iso(), operation=operation,
                added_files=added_ids, removed_files=tuple(sorted(removed_ids)),
            )
            meta.schemas[new_schema.version] = new_schema
            meta.snapshots[new_version] = snapshot
            meta.current_version = new_version

            self._write_version_record(table, snapshot, new_schema,
                                       list(added) + backfill_rewritten)
            self.storage.atomic_write_json(
                self.storage.metadata_path(table), meta.to_dict())
            ledger.append(LedgerEntry(
                commit_id=commit_id, version=new_version, status="applied",
                parent_version=current, timestamp=snapshot.timestamp))

        log.info(
            "commit accepted table=%s commit_id=%s base_version=%s "
            "version=%s schema_version=%s operation=%s added=%d removed=%d "
            "decision=accept reason=base_current",
            table, commit_id, base_version, new_version, new_schema.version,
            operation, len(added_ids), len(removed_ids))
        return self._commit_result(meta, new_version)

    # =================================================================
    # Reads
    # =================================================================
    def read_table(
        self, table: str, *, version: int | None = None,
        commit_id: str | None = None,
        partitions: dict[str, str] | None = None,
    ) -> dict[str, object]:
        with self.storage.table_lock(table):
            meta = self._load(table)
            snapshot = self._resolve_snapshot(meta, version, commit_id)
            schema = meta.schemas[snapshot.schema_version]

            filters = dict(partitions or {})
            unknown = [k for k in filters if k not in snapshot.partition_spec]
            if unknown:
                raise ValidationError(
                    f"partition keys {sorted(unknown)} do not exist in the "
                    f"partition spec of snapshot {snapshot.version}",
                    reason="unknown_partition_for_snapshot",
                    snapshot_version=snapshot.version,
                    partition_spec=list(snapshot.partition_spec),
                )

            files_out: list[dict[str, object]] = []
            all_rows: list[dict[str, object]] = []
            for fid in snapshot.file_ids:
                df = self._read_file(table, fid)
                pv = df.partition_values
                if any(str(pv.get(k)) != str(v) for k, v in filters.items()):
                    continue
                rows = [_project_row(r, schema) for r in df.rows]
                all_rows.extend(rows)
                files_out.append({
                    "file_id": fid,
                    "partition_values": dict(pv),
                    "schema_version": df.schema_version,
                    "row_count": len(rows),
                })
        log.info("read table=%s snapshot_version=%s commit_id=%s "
                 "partitions=%s files=%d rows=%d decision=read "
                 "reason=snapshot_resolved",
                 table, snapshot.version, snapshot.commit_id, filters or {},
                 len(files_out), len(all_rows))
        return {
            "table": table,
            "version": snapshot.version,
            "commit_id": snapshot.commit_id,
            "schema_version": schema.version,
            "partition_spec": list(snapshot.partition_spec),
            "schema": schema.to_dict(),
            "selected_partitions": filters,
            "files": files_out,
            "row_count": len(all_rows),
            "rows": all_rows,
        }

    def get_snapshot(
        self, table: str, *, version: int | None = None,
        commit_id: str | None = None,
    ) -> dict[str, object]:
        with self.storage.table_lock(table):
            meta = self._load(table)
            snap = self._resolve_snapshot(meta, version, commit_id)
            out = snap.to_dict()
            out["retained"] = True
            out["current"] = snap.version == meta.current_version
            return out

    def list_snapshots(self, table: str) -> dict[str, object]:
        with self.storage.table_lock(table):
            meta = self._load(table)
            ledger = self._ledger(table)
            live = {v for v in meta.snapshots}
            expired = sorted({e.version for e in ledger.all_entries()} - live)
            return {
                "table": table,
                "current_version": meta.current_version,
                "snapshots": [meta.snapshots[v].to_dict()
                              for v in sorted(meta.snapshots)],
                "expired_versions": expired,
            }

    def tag_snapshot(self, table: str, version: int) -> dict[str, object]:
        with self.storage.table_lock(table):
            meta = self._load(table)
            snap = self._resolve_snapshot(meta, version, None)
            state = self._load_retention(table)
            tags = set(state.get("tags", ()))
            tags.add(snap.version)
            state["tags"] = sorted(tags)
            self.storage.atomic_write_json(self.storage.retention_path(table), state)
        log.info("snapshot tagged table=%s version=%s decision=tag", table, snap.version)
        return {"table": table, "version": snap.version, "tags": sorted(tags)}

    # =================================================================
    # Retention / cleanup
    # =================================================================
    def expire_snapshots(
        self, table: str, policy: dict[str, object] | None = None,
    ) -> dict[str, object]:
        with self.storage.table_lock(table):
            meta = self._load(table)
            persisted = self._load_retention(table)
            if policy is not None:
                # Persist the explicit policy for future runs.
                resolved = RetentionPolicy.from_dict(policy)
                state = dict(persisted)
                state["policy"] = resolved.to_dict()
                self.storage.atomic_write_json(
                    self.storage.retention_path(table), state)
            else:
                resolved = RetentionPolicy.from_dict(persisted.get("policy"))
            # Tags always come from the persisted retention state.
            resolved = RetentionPolicy(
                min_snapshots=resolved.min_snapshots,
                max_age_seconds=resolved.max_age_seconds,
                tags=frozenset(int(v) for v in persisted.get("tags", ())),
            )

            timestamps = {
                v: parse_ts(s.timestamp) for v, s in meta.snapshots.items()
            }
            expired = select_expired(
                list(meta.snapshots), timestamps, meta.current_version, resolved)

            deleted_files: list[str] = []
            for v in expired:
                snap = meta.snapshots.pop(v)
                # GC schemas no longer referenced by any retained snapshot.
                used = {s.schema_version for s in meta.snapshots.values()}
                for sv in list(meta.schemas):
                    if sv not in used:
                        del meta.schemas[sv]
                # Delete data files exclusively owned by removed snapshots.
                retained_file_ids = {
                    fid for s in meta.snapshots.values()
                    for fid in s.file_ids
                }
                for fid in list(snap.file_ids):
                    if fid not in retained_file_ids:
                        self.storage.delete_data_file(table, fid)
                        deleted_files.append(fid)
                self.storage.version_path(table, v).unlink(missing_ok=True)

            self.storage.atomic_write_json(
                self.storage.metadata_path(table), meta.to_dict())
        log.info("expire table=%s expired_versions=%s deleted_files=%d "
                 "retained_versions=%s decision=cleanup reason=retention_policy",
                 table, expired, len(deleted_files),
                 sorted(meta.snapshots))
        return {
            "table": table,
            "current_version": meta.current_version,
            "expired_versions": expired,
            "retained_versions": sorted(meta.snapshots),
            "deleted_files": sorted(deleted_files),
            "policy": resolved.to_dict(),
        }

    # =================================================================
    # Internal helpers
    # =================================================================
    def _load(self, table: str) -> TableMetadata:
        path = self.storage.metadata_path(table)
        if not path.exists():
            raise TableNotFoundError(f"table {table!r} not found", table=table)
        data = self.storage.read_json(path)
        return TableMetadata.from_dict(data, location=str(self.storage.table_dir(table)))  # type: ignore[arg-type]

    def _ledger(self, table: str) -> CommitLedger:
        return CommitLedger(self.storage.ledger_path(table))

    def _find_ledger(self, table: str, commit_id: str) -> LedgerEntry | None:
        return self._ledger(table).find_commit(commit_id)

    def _check_base(
        self, table: str, meta: TableMetadata, base: int, current: int,
    ) -> int:
        if base == current:
            return base
        if base > current:
            raise ValidationError(
                f"base_version {base} is ahead of current version {current}",
                reason="base_version_ahead_of_current",
                base_version=base, current_version=current)
        # base < current
        if base in meta.snapshots:
            log.warning("commit rejected table=%s base_version=%s "
                        "current_version=%s decision=reject "
                        "reason=concurrent_commit_conflict",
                        table, base, current)
            raise ConcurrentCommitConflictError(
                f"base version {base} is no longer current; version {current} "
                "was committed concurrently",
                reason="base_version_not_current",
                base_version=base, current_version=current)
        if self._ledger(table).is_known_version(base):
            log.warning("commit rejected table=%s base_version=%s "
                        "current_version=%s decision=reject "
                        "reason=base_version_expired",
                        table, base, current)
            raise StaleBaseError(
                f"base version {base} has been reclaimed by retention and "
                f"cannot be a commit base (current version: {current})",
                reason="base_version_expired",
                base_version=base, current_version=current)
        raise ValidationError(
            f"base version {base} never existed on table {table!r}",
            reason="unknown_base_version", base_version=base,
            current_version=current)

    def _resolve_snapshot(
        self, meta: TableMetadata, version: int | None,
        commit_id: str | None,
    ) -> Snapshot:
        if version is not None and commit_id is not None:
            raise ValidationError(
                "specify either version or commit_id, not both",
                reason="ambiguous_snapshot_ref")
        target_version: int | None = None
        if commit_id is not None:
            entry = self._ledger(meta.name).find_commit(commit_id)
            if entry is None:
                raise SnapshotNotFoundError(
                    f"no commit with id {commit_id!r}",
                    reason="unknown_commit_id", commit_id=commit_id)
            target_version = entry.version
        elif version is not None:
            if not isinstance(version, int) or isinstance(version, bool):
                raise ValidationError("version must be an integer",
                                      field="version")
            target_version = version
        else:
            target_version = meta.current_version

        snap = meta.snapshots.get(target_version)
        if snap is not None:
            return snap
        if self._ledger(meta.name).is_known_version(target_version):  # type: ignore[arg-type]
            raise SnapshotExpiredError(
                f"snapshot version {target_version} existed but has been "
                "reclaimed by retention; tag a snapshot to keep it readable",
                reason="snapshot_expired", version=target_version)
        raise SnapshotNotFoundError(
            f"snapshot version {target_version} never existed",
            reason="unknown_version", version=target_version)

    def _build_schema(
        self, columns: list[dict[str, object]], *, version: int,
    ) -> Schema:
        if not isinstance(columns, list) or not columns:
            raise ValidationError("a table needs at least one column",
                                  field="columns")
        built: list[Column] = []
        seen: set[str] = set()
        for raw in columns:
            if not isinstance(raw, dict):
                raise ValidationError("each column must be an object")
            name = raw.get("name")
            if not isinstance(name, str) or not name:
                raise ValidationError("column requires non-empty 'name'",
                                      field="name")
            if name in seen:
                raise ValidationError(f"duplicate column name {name!r}",
                                      reason="duplicate_column", column=name)
            seen.add(name)
            type_name = datatypes.validate_type(str(raw.get("type", "")))
            nullable = bool(raw.get("nullable", True))
            default = raw.get("default")
            if default is not None:
                default = datatypes.coerce_value(default, type_name)
            # Note: at CREATE time a non-nullable column needs no default
            # (writers always supply values). Only a column *added by
            # evolution* must carry a deterministic default -- that check
            # lives in schema.validate_evolution.
            built.append(Column(name=name, type=type_name, nullable=nullable,
                                default=default, doc=str(raw.get("doc", ""))))
        return Schema(columns=tuple(built), version=version)

    @staticmethod
    def _validate_partition_spec(
        partition_by: list[str] | None, schema: Schema,
    ) -> tuple[str, ...]:
        if not partition_by:
            return ()
        if not isinstance(partition_by, list) \
                or not all(isinstance(p, str) for p in partition_by):
            raise ValidationError("partition_by must be a list of column names",
                                  field="partition_by")
        names = schema.names
        for p in partition_by:
            if p not in names:
                raise ValidationError(
                    f"partition column {p!r} is not in the schema",
                    reason="unknown_partition_column", column=p,
                    known=list(names))
        if len(set(partition_by)) != len(partition_by):
            raise ValidationError("duplicate partition columns",
                                  reason="duplicate_partition_column")
        return tuple(partition_by)

    def _build_data_file(
        self, table: str, new_version: int, schema: Schema,
        group: dict[str, object], spec: tuple[str, ...],
    ) -> DataFile:
        if not isinstance(group, dict):
            raise ValidationError("each write group must be an object")
        raw_rows = group.get("rows", [])
        if not isinstance(raw_rows, list):
            raise ValidationError("'rows' must be a list", field="rows")
        partition = group.get("partition", {})
        if not isinstance(partition, dict):
            raise ValidationError("'partition' must be an object",
                                  field="partition")
        pv = {str(k): str(v) for k, v in partition.items()}
        extra = [k for k in pv if k not in spec]
        missing = [k for k in spec if k not in pv]
        if extra:
            raise ValidationError(
                f"partition values {sorted(extra)} are not in the partition "
                "spec", reason="unknown_partition_column",
                partition_spec=list(spec))
        if missing and raw_rows:
            raise ValidationError(
                f"rows require partition values for {sorted(missing)}",
                reason="missing_partition_value", missing=missing)
        rows = tuple(validate_row(schema, r) for r in raw_rows)
        file_id = f"v{new_version:06d}-{uuid.uuid4().hex[:12]}"
        return DataFile(file_id, rows, schema.version, pv)

    def _read_file(self, table: str, file_id: str) -> DataFile:
        try:
            return self.storage.read_data_file(table, file_id)
        except FileNotFoundError:
            raise DataFileNotFoundError(
                f"data file {file_id!r} is missing; the snapshot referencing "
                "it may have been corrupted or partially cleaned",
                file_id=file_id) from None

    def _write_version_record(
        self, table: str, snapshot: Snapshot, schema: Schema,
        data_files: list[DataFile],
    ) -> None:
        payload = {
            "snapshot": snapshot.to_dict(),
            "schema": schema.to_dict(),
            "data_files": [df.to_dict() for df in data_files],
        }
        self.storage.atomic_write_json(
            self.storage.version_path(table, snapshot.version), payload)

    def _load_retention(self, table: str) -> dict[str, object]:
        path = self.storage.retention_path(table)
        if path.exists():
            data = self.storage.read_json(path)
            return data if isinstance(data, dict) else {}  # type: ignore[return-value]
        return {}

    def _commit_result(
        self, meta: TableMetadata, version: int, *,
        idempotent: bool = False, reason: str = "accepted",
    ) -> dict[str, object]:
        snap = meta.snapshots[version]
        return {
            "table": meta.name,
            "commit_id": snap.commit_id,
            "version": snap.version,
            "parent_version": snap.parent_version,
            "schema_version": snap.schema_version,
            "operation": snap.operation,
            "added_files": list(snap.added_files),
            "removed_files": list(snap.removed_files),
            "current_version": meta.current_version,
            "idempotent_retry": idempotent,
            "decision": "duplicate_commit" if idempotent else "accepted",
            "reason": reason,
        }


# ---------------------------------------------------------------------
# module-level helpers
# ---------------------------------------------------------------------
def _commit_id(commit_id: str | None) -> str:
    if commit_id is None:
        return uuid.uuid4().hex
    if not isinstance(commit_id, str) or not commit_id.strip():
        raise ValidationError("commit_id must be a non-empty string",
                              field="commit_id")
    return commit_id


def _extract_groups(request: dict[str, object], key: str) -> list[dict[str, object]]:
    value = request.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(g, dict) for g in value):
        raise ValidationError(f"{key!r} must be a list of group objects",
                              field=key)
    return list(value)


def _operation_name(
    plan: EvolutionPlan | None,
    append: list[dict[str, object]],
    overwrite: list[dict[str, object]],
) -> str:
    parts: list[str] = []
    if overwrite:
        parts.append("overwrite")
    if append:
        parts.append("append")
    if plan is not None:
        parts.append("schema_update")
    return "+".join(parts) if parts else "noop"


def _project_row(row: dict[str, object], schema: Schema) -> dict[str, object]:
    """Project a stored row (possibly written under an older schema) onto
    ``schema``, materializing defaults for added columns and normalizing
    widened numeric values."""
    out: dict[str, object] = {}
    for col in schema.columns:
        value = row.get(col.name, col.default)
        if value is not None:
            value = datatypes.coerce_value(value, col.type)
        out[col.name] = value
    return out
