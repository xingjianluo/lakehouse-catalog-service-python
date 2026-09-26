"""Table schema: columns, evolution validation and row validation.

Compatibility philosophy (see docs/architecture.md):

* **Adding** a column is allowed only when its value for existing rows is
  deterministic: either the column is nullable (implicit ``null`` default)
  or an explicit, type-checked ``default`` is given. Two backfill policies
  define *when* that default is realized -- ``on_read`` (old files are
  projected at read time) or ``materialized`` (a new snapshot rewrites the
  live files with the default baked in).
* **Widening** a type along ``int -> long -> double`` is allowed.
* **Narrowing**, cross-family type changes, **dropping** and **renaming**
  are rejected with distinct reasons, because any of them can make
  historical bytes silently read as another column/value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from . import datatypes
from .errors import InvalidDefaultError, SchemaIncompatibleError, ValidationError

BackfillPolicy = Literal["on_read", "materialized"]
_BACKFILL_POLICIES = frozenset({"on_read", "materialized"})


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    default: object | None = None
    doc: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "default": self.default,
            "doc": self.doc,
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> Column:
        return cls(
            name=str(d["name"]),
            type=str(d["type"]),
            nullable=bool(d.get("nullable", True)),
            default=d.get("default"),
            doc=str(d.get("doc", "")),
        )


@dataclass(frozen=True)
class Schema:
    columns: tuple[Column, ...] = field(default_factory=tuple)
    version: int = 1

    def column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "columns": [c.to_dict() for c in self.columns],
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> Schema:
        return cls(
            columns=tuple(Column.from_dict(c) for c in d.get("columns", ())),  # type: ignore[arg-type]
            version=int(d.get("version", 1)),
        )


@dataclass(frozen=True)
class EvolutionPlan:
    """A validated schema change that is safe to apply."""

    action: Literal["add_column", "widen_type"]
    column: Column
    backfill: BackfillPolicy = "on_read"
    target_name: str | None = None


def _check_default(column: Column) -> None:
    """Ensure the declared default matches the column type."""
    if column.default is None:
        if not column.nullable:
            raise SchemaIncompatibleError(
                f"new non-nullable column {column.name!r} needs a default",
                reason="missing_default_for_new_column",
                column=column.name,
            )
        return
    try:
        datatypes.coerce_value(column.default, column.type)
    except ValidationError as exc:
        raise InvalidDefaultError(
            f"default for column {column.name!r} does not match type "
            f"{column.type}: {exc.message}",
            reason="default_type_mismatch",
            column=column.name,
            declared_type=column.type,
        ) from None


def validate_evolution(old: Schema, change: dict[str, object]) -> EvolutionPlan:
    """Validate a single schema change against ``old``.

    Raises :class:`SchemaIncompatibleError` (with a distinct ``reason``)
    or :class:`ValidationError` for malformed requests.
    """
    action = change.get("action")
    if not isinstance(action, str):
        raise ValidationError("schema change requires string 'action'", field="action")

    if action == "add_column":
        name = change.get("name")
        if not isinstance(name, str) or not name:
            raise ValidationError("add_column requires non-empty 'name'", field="name")
        if old.column(name) is not None:
            raise SchemaIncompatibleError(
                f"column {name!r} already exists at schema version {old.version}",
                reason="column_already_exists",
                column=name,
            )
        type_name = datatypes.validate_type(str(change.get("type", "")))
        nullable = bool(change.get("nullable", True))
        has_default = "default" in change and change["default"] is not None
        default = change.get("default")
        column = Column(
            name=name,
            type=type_name,
            nullable=nullable,
            # Keep the raw default here; _check_default reports a typed
            # InvalidDefaultError (distinct from a generic validation error).
            default=default,
            doc=str(change.get("doc", "")),
        )
        _check_default(column)
        # Normalize the now-validated default.
        if column.default is not None:
            column = Column(name=column.name, type=column.type,
                            nullable=column.nullable,
                            default=datatypes.coerce_value(column.default,
                                                           column.type),
                            doc=column.doc)
        backfill = change.get("backfill", "on_read")
        if backfill not in _BACKFILL_POLICIES:
            raise ValidationError(
                f"invalid backfill policy {backfill!r}",
                field="backfill",
                allowed=sorted(_BACKFILL_POLICIES),
            )
        if not nullable and not has_default:
            raise SchemaIncompatibleError(
                f"new non-nullable column {name!r} requires an explicit default",
                reason="missing_default_for_new_column",
                column=name,
            )
        return EvolutionPlan(action="add_column", column=column,
                             backfill=backfill)  # type: ignore[arg-type]

    if action == "drop_column":
        name = change.get("name")
        if not isinstance(name, str) or old.column(name) is None:
            raise SchemaIncompatibleError(
                f"cannot drop unknown column {name!r}",
                reason="column_not_found",
                column=name,
            )
        raise SchemaIncompatibleError(
            f"dropping column {name!r} is rejected: historical data files would "
            "silently lose a field",
            reason="column_drop_not_supported",
            column=name,
        )

    if action == "rename_column":
        name = change.get("name")
        if not isinstance(name, str) or old.column(name) is None:
            raise SchemaIncompatibleError(
                f"cannot rename unknown column {name!r}",
                reason="column_not_found",
                column=name,
            )
        raise SchemaIncompatibleError(
            f"renaming column {name!r} is rejected: position/name based readers "
            "would silently misalign historical rows",
            reason="column_rename_not_supported",
            column=name,
        )

    if action in ("widen_type", "update_type", "change_type"):
        name = change.get("name")
        existing = old.column(name) if isinstance(name, str) else None
        if existing is None:
            raise SchemaIncompatibleError(
                f"cannot change type of unknown column {name!r}",
                reason="column_not_found",
                column=name,
            )
        new_type = datatypes.validate_type(str(change.get("type", "")))
        if new_type == existing.type:
            return EvolutionPlan(
                action="widen_type",
                column=existing,
                target_name=existing.name,
            )
        if not datatypes.can_widen(existing.type, new_type):
            narrowing = new_type in datatypes._NUMERIC_ORDER and \
                datatypes._NUMERIC_ORDER.get(new_type, -1) < datatypes._NUMERIC_ORDER.get(existing.type, -1)
            reason = "type_narrowing" if narrowing else "incompatible_type_change"
            raise SchemaIncompatibleError(
                f"cannot change column {name!r} from {existing.type!r} to "
                f"{new_type!r}: the change may truncate or reinterpret history",
                reason=reason,
                column=name,
                old_type=existing.type,
                new_type=new_type,
            )
        # Existing default (if any) must remain valid under the wider type.
        widened = Column(name=existing.name, type=new_type,
                         nullable=existing.nullable,
                         default=datatypes.coerce_value(existing.default, new_type)
                         if existing.default is not None else None,
                         doc=existing.doc)
        return EvolutionPlan(action="widen_type", column=widened,
                             target_name=existing.name)

    raise ValidationError(
        f"unknown schema action {action!r}",
        field="action",
        allowed=["add_column", "drop_column", "rename_column", "widen_type"],
    )


def apply_add(old: Schema, column: Column) -> Schema:
    """Return a new schema with ``column`` appended."""
    return Schema(columns=old.columns + (column,), version=old.version + 1)


def apply_widen(old: Schema, target_name: str, column: Column) -> Schema:
    """Return a new schema with one column replaced by its widened form."""
    cols = tuple(column if c.name == target_name else c for c in old.columns)
    return Schema(columns=cols, version=old.version + 1)


def validate_row(schema: Schema, row: dict[str, object]) -> dict[str, object]:
    """Validate one write row against ``schema``; return coerced values.

    Unknown columns are rejected (rather than dropped) to avoid silent
    misalignment, and non-nullable columns must receive a non-null value
    or a usable default.
    """
    if not isinstance(row, dict):
        raise ValidationError("row must be a JSON object", field="row")
    known = set(schema.names)
    unknown = [k for k in row if k not in known]
    if unknown:
        raise ValidationError(
            f"row contains unknown columns {sorted(unknown)}",
            reason="unknown_column",
            unknown=sorted(unknown),
            known=list(schema.names),
        )
    coerced: dict[str, object] = {}
    for col in schema.columns:
        if col.name in row:
            value = datatypes.coerce_value(row[col.name], col.type)
        else:
            value = col.default
        if value is None and not col.nullable:
            raise ValidationError(
                f"column {col.name!r} is non-nullable but no value provided",
                reason="null_in_non_nullable_column",
                column=col.name,
            )
        coerced[col.name] = value
    return coerced
