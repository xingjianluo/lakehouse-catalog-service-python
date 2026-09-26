"""In-memory model objects: data files, snapshots, table metadata."""

from __future__ import annotations

from dataclasses import dataclass, field

from .schema import Schema


@dataclass(frozen=True)
class DataFile:
    """A local data file belonging to a table.

    ``schema_version`` records the schema at write time. Rows are stored as
    plain dicts; columns added later are filled at read time (backfill
    ``on_read``) or baked into new files (backfill ``materialized``).
    """

    file_id: str
    rows: tuple[dict[str, object], ...]
    schema_version: int
    partition_values: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "file_id": self.file_id,
            "schema_version": self.schema_version,
            "partition_values": dict(self.partition_values),
            "rows": [dict(r) for r in self.rows],
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> DataFile:
        return cls(
            file_id=str(d["file_id"]),
            schema_version=int(d["schema_version"]),
            partition_values={str(k): str(v)
                              for k, v in dict(d.get("partition_values", {})).items()},
            rows=tuple(dict(r) for r in d.get("rows", ())),  # type: ignore[arg-type]
        )


@dataclass(frozen=True)
class Snapshot:
    version: int
    commit_id: str
    parent_version: int | None
    schema_version: int
    file_ids: tuple[str, ...]
    partition_spec: tuple[str, ...]
    timestamp: str
    operation: str
    added_files: tuple[str, ...] = ()
    removed_files: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "commit_id": self.commit_id,
            "parent_version": self.parent_version,
            "schema_version": self.schema_version,
            "file_ids": list(self.file_ids),
            "partition_spec": list(self.partition_spec),
            "timestamp": self.timestamp,
            "operation": self.operation,
            "added_files": list(self.added_files),
            "removed_files": list(self.removed_files),
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> Snapshot:
        def ids(key: str) -> tuple[str, ...]:
            return tuple(str(x) for x in d.get(key, ()))  # type: ignore[arg-type]

        pv = d.get("parent_version")
        return cls(
            version=int(d["version"]),
            commit_id=str(d["commit_id"]),
            parent_version=int(pv) if pv is not None else None,
            schema_version=int(d["schema_version"]),
            file_ids=ids("file_ids"),
            partition_spec=tuple(str(x) for x in d.get("partition_spec", ())),  # type: ignore[arg-type]
            timestamp=str(d["timestamp"]),
            operation=str(d["operation"]),
            added_files=ids("added_files"),
            removed_files=ids("removed_files"),
        )


@dataclass
class TableMetadata:
    name: str
    location: str
    current_version: int
    schemas: dict[int, Schema] = field(default_factory=dict)
    snapshots: dict[int, Snapshot] = field(default_factory=dict)
    partition_spec: tuple[str, ...] = ()

    def current_snapshot(self) -> Snapshot:
        return self.snapshots[self.current_version]

    def current_schema(self) -> Schema:
        return self.schemas[self.current_snapshot().schema_version]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "location": self.location,
            "current_version": self.current_version,
            "partition_spec": list(self.partition_spec),
            "schemas": {str(v): s.to_dict() for v, s in sorted(self.schemas.items())},
            "snapshots": {
                str(v): snap.to_dict()
                for v, snap in sorted(self.snapshots.items())
            },
        }

    @classmethod
    def from_dict(cls, d: dict[str, object], *, location: str) -> TableMetadata:
        schemas = {int(k): Schema.from_dict(v) for k, v in dict(d["schemas"]).items()}  # type: ignore[arg-type]
        snapshots = {
            int(k): Snapshot.from_dict(v)
            for k, v in dict(d["snapshots"]).items()  # type: ignore[arg-type]
        }
        return cls(
            name=str(d["name"]),
            location=location,
            current_version=int(d["current_version"]),
            schemas=schemas,
            snapshots=snapshots,
            partition_spec=tuple(str(x) for x in d.get("partition_spec", ())),  # type: ignore[arg-type]
        )
