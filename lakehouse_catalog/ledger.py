"""Append-only commit ledger.

One ``ledger.jsonl`` file per table records every commit that ever applied:

* **idempotent retries** -- a retried ``commit_id`` is found here and the
  original result is returned instead of being applied twice;
* **expired vs unknown versions** -- when retention deletes a snapshot and
  its data files, the ledger entry remains as a tombstone. A read at that
  version therefore gets ``snapshot_expired`` (HTTP 410), while a version
  that never existed gets ``snapshot_not_found`` (HTTP 404).

Entries are appended with ``O_APPEND`` and an immediate fsync, which is
atomic for small writes on local POSIX file systems.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LedgerEntry:
    commit_id: str
    version: int
    status: str  # currently always "applied"
    parent_version: int | None
    timestamp: str

    def to_dict(self) -> dict[str, object]:
        return {
            "commit_id": self.commit_id,
            "version": self.version,
            "status": self.status,
            "parent_version": self.parent_version,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> LedgerEntry:
        pv = d.get("parent_version")
        return cls(
            commit_id=str(d["commit_id"]),
            version=int(d["version"]),
            status=str(d.get("status", "applied")),
            parent_version=int(pv) if pv is not None else None,
            timestamp=str(d["timestamp"]),
        )


class CommitLedger:
    """Append-only, fsynced JSON-lines ledger file per table."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, entry: LedgerEntry) -> None:
        line = json.dumps(entry.to_dict(), ensure_ascii=False, sort_keys=True)
        # Low-level append write: one write() of < PIPE_BUF-sized record is
        # atomic; the table lock additionally serializes writers anyway.
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, (line + "\n").encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)

    def all_entries(self) -> list[LedgerEntry]:
        if not self.path.exists():
            return []
        out: list[LedgerEntry] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(LedgerEntry.from_dict(json.loads(line)))
        return out

    def find_commit(self, commit_id: str) -> LedgerEntry | None:
        for entry in self.all_entries():
            if entry.commit_id == commit_id:
                return entry
        return None

    def is_known_version(self, version: int) -> bool:
        return any(e.version == version for e in self.all_entries())
