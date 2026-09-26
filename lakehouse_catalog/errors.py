"""Error hierarchy.

Every user-facing failure carries a stable ``code`` so that callers
(HTTP layer, tests) can distinguish failure *reasons* rather than
parsing exception messages.
"""

from __future__ import annotations


class CatalogError(Exception):
    """Base class for all catalog errors."""

    code: str = "catalog_error"
    http_status: int = 400

    def __init__(self, message: str, *, code: str | None = None,
                 http_status: int | None = None, **details: object) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if http_status is not None:
            self.http_status = http_status
        self.details: dict[str, object] = details

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {"error": self.code, "message": self.message}
        d.update(self.details)
        return d


class TableNotFoundError(CatalogError):
    code = "table_not_found"
    http_status = 404


class TableAlreadyExistsError(CatalogError):
    code = "table_already_exists"
    http_status = 409


class ConcurrentCommitConflictError(CatalogError):
    """Commit rejected because the base version is no longer current."""

    code = "concurrent_commit_conflict"
    http_status = 409


class StaleBaseError(CatalogError):
    """Commit based on an explicitly expired / reclaimed version."""

    code = "stale_base_version"
    http_status = 409


class DuplicateCommitError(CatalogError):
    """Idempotent retry: a commit with the same commit_id already applied."""

    code = "duplicate_commit"
    http_status = 200


class SnapshotNotFoundError(CatalogError):
    code = "snapshot_not_found"
    http_status = 404


class SnapshotExpiredError(CatalogError):
    """Version is known to have existed but has been reclaimed."""

    code = "snapshot_expired"
    http_status = 410


class SchemaIncompatibleError(CatalogError):
    code = "schema_incompatible"
    http_status = 422


class InvalidDefaultError(CatalogError):
    code = "invalid_default"
    http_status = 422


class ValidationError(CatalogError):
    code = "validation_error"
    http_status = 400


class DataFileNotFoundError(CatalogError):
    code = "data_file_not_found"
    http_status = 410
