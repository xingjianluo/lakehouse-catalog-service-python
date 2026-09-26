"""File-system backed storage: atomic writes, locks and persistence.

On-disk layout per table (``<root>/<table>/``)::

    metadata.json                current pointer + all live snapshots/schemas
    versions/vNNNNNN.json        immutable version record (audit/history)
    data/<file_id>.json          immutable data files (rows)
    ledger.jsonl                 append-only commit ledger (outcomes/tombstones)
    table.lock                   fcntl flock target
    retention.json               retention policy + explicit snapshot tags

Durability/atomicity:

* every document is written to a temp file in the same directory, fsynced
  and then ``os.replace``-d into place -- readers always observe either the
  old or the new complete file, never a partial one;
* commits take an exclusive per-table lock (in-process ``RLock`` plus an
  inter-process ``flock``), so the check-then-act commit sequence is
  serialized for every entry point;
* data/version files are written *before* the metadata pointer flips.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path

from .model import DataFile


class FileStorage:
    """Persist JSON documents and row data under a warehouse directory."""

    #: One in-process lock per table; flock serializes across processes.
    _process_locks: dict[str, threading.RLock] = {}
    _process_locks_guard = threading.Lock()

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # Lock files live outside table directories so that acquiring a lock
        # never implicitly creates a table (and survives a dropped table).
        self.locks_dir = self.root / ".locks"
        self.locks_dir.mkdir(exist_ok=True)

    # ---- paths -----------------------------------------------------
    def table_dir(self, table: str) -> Path:
        _validate_table_name(table)
        return self.root / table

    def metadata_path(self, table: str) -> Path:
        return self.table_dir(table) / "metadata.json"

    def versions_dir(self, table: str) -> Path:
        return self.table_dir(table) / "versions"

    def version_path(self, table: str, version: int) -> Path:
        return self.versions_dir(table) / f"v{version:06d}.json"

    def data_dir(self, table: str) -> Path:
        return self.table_dir(table) / "data"

    def data_path(self, table: str, file_id: str) -> Path:
        _validate_file_id(file_id)
        return self.data_dir(table) / f"{file_id}.json"

    def ledger_path(self, table: str) -> Path:
        return self.table_dir(table) / "ledger.jsonl"

    def retention_path(self, table: str) -> Path:
        return self.table_dir(table) / "retention.json"

    def lock_path(self, table: str) -> Path:
        return self.locks_dir / f"{table}.lock"

    # ---- table lifecycle ------------------------------------------
    def init_table_dirs(self, table: str) -> None:
        tdir = self.table_dir(table)
        if tdir.exists():
            raise FileExistsError(table)
        for sub in (tdir, self.versions_dir(table), self.data_dir(table)):
            sub.mkdir(parents=True, exist_ok=False)

    def table_exists(self, table: str) -> bool:
        return self.metadata_path(table).exists()

    def list_tables(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            p.name for p in self.root.iterdir()
            if p.is_dir() and (p / "metadata.json").exists()
        )

    def delete_table(self, table: str) -> None:
        import shutil

        shutil.rmtree(self.table_dir(table))

    # ---- atomic io -------------------------------------------------
    @staticmethod
    def atomic_write_json(path: Path, payload: object) -> None:
        """Durably write JSON: temp file in same dir + fsync + atomic rename."""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp",
                                        dir=path.parent)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2,
                          sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            # Best-effort directory entry durability.
            try:
                dir_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:  # pragma: no cover - platform dependent
                pass
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    @staticmethod
    def read_json(path: Path) -> object:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    # ---- data files ------------------------------------------------
    def write_data_file(self, table: str, data_file: DataFile) -> None:
        self.atomic_write_json(self.data_path(table, data_file.file_id),
                               data_file.to_dict())

    def read_data_file(self, table: str, file_id: str) -> DataFile:
        path = self.data_path(table, file_id)
        if not path.exists():
            raise FileNotFoundError(file_id)
        return DataFile.from_dict(self.read_json(path))  # type: ignore[arg-type]

    def delete_data_file(self, table: str, file_id: str) -> None:
        self.data_path(table, file_id).unlink(missing_ok=True)

    # ---- locks -----------------------------------------------------
    @contextlib.contextmanager
    def table_lock(self, table: str) -> Iterator[None]:
        """Exclusive per-table lock (in-process + inter-process flock)."""
        key = str(self.root.resolve() / table)
        with self._process_locks_guard:
            lock = self._process_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._process_locks[key] = lock
        self.lock_path(table).parent.mkdir(parents=True, exist_ok=True)
        self.lock_path(table).touch(exist_ok=True)
        with lock:
            f = self.lock_path(table).open("a+")
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                finally:
                    f.close()


def _validate_table_name(table: str) -> None:
    if not isinstance(table, str) or not table \
            or table in (".", "..") or "/" in table or os.sep in table:
        raise ValueError(f"invalid table name: {table!r}")


def _validate_file_id(file_id: str) -> None:
    if not isinstance(file_id, str) or not file_id \
            or "/" in file_id or file_id in (".", "..") or file_id.startswith("."):
        raise ValueError(f"invalid file id: {file_id!r}")
