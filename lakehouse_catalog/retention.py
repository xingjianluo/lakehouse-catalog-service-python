"""Snapshot retention policy and expiration selection.

A snapshot survives cleanup when *any* of the following holds:

1. it is the current snapshot (never expires);
2. it is pinned by an explicit tag (long-lived references);
3. it is among the most recent ``min_snapshots`` versions;
4. it is younger than ``max_age_seconds``.

Everything else may be reclaimed. Actual file deletion lives in the
catalog (it must hold the table lock and update metadata + tombstones
atomically); this module only computes the deterministic selection so it
can be unit-tested in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


def _utcnow() -> datetime:
    return datetime.now(UTC)


def parse_ts(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp, accepting a trailing ``Z``."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


@dataclass(frozen=True)
class RetentionPolicy:
    min_snapshots: int = 5
    max_age_seconds: float | None = None
    tags: frozenset[int] = field(default_factory=frozenset)

    @classmethod
    def from_dict(cls, d: dict[str, object] | None) -> RetentionPolicy:
        if not d:
            return cls()
        min_snaps = int(d.get("min_snapshots", 5))
        min_snaps = max(min_snaps, 1)
        age = d.get("max_age_seconds")
        return cls(
            min_snapshots=min_snaps,
            max_age_seconds=float(age) if age is not None else None,
            tags=frozenset(int(v) for v in d.get("tags", ())),  # type: ignore[arg-type]
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "min_snapshots": self.min_snapshots,
            "max_age_seconds": self.max_age_seconds,
            "tags": sorted(self.tags),
        }


def select_expired(
    versions: list[int],
    timestamps: dict[int, datetime],
    current_version: int,
    policy: RetentionPolicy,
    now: datetime | None = None,
) -> list[int]:
    """Return versions safe to expire; sorted ascending.

    ``versions`` is every live snapshot version. Newest versions are
    retained first when applying ``min_snapshots``.
    """
    now = now or _utcnow()
    ordered = sorted(versions, reverse=True)
    # The most-recent min_snapshots are protected (current is in there too).
    age_protected: set[int] = set(ordered[: max(1, policy.min_snapshots)])
    expired: list[int] = []
    for v in ordered:
        if v == current_version:
            continue
        if v in policy.tags:
            continue
        if v in age_protected:
            continue
        if policy.max_age_seconds is not None:
            ts = timestamps.get(v)
            if ts is None:
                # Missing timestamp -> keep rather than risk deleting a
                # still-referenced version.
                continue
            if (now - ts.astimezone(UTC)).total_seconds() < policy.max_age_seconds:
                continue
        expired.append(v)
    return sorted(expired)
