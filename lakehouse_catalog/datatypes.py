"""Supported column data types, widening rules and value coercion.

The type system deliberately stays small so that compatibility decisions
are deterministic and easy to explain:

* ``int`` -> ``long`` -> ``double`` is the only implicit widening chain;
* ``string`` and ``boolean`` only match themselves;
* any other transition (narrowing, cross-family changes such as
  ``string`` -> ``int``) is rejected by schema evolution.
"""

from __future__ import annotations

from .errors import ValidationError

#: Numeric types ordered by representational width.
_NUMERIC_ORDER: dict[str, int] = {"int": 0, "long": 1, "double": 2}
SUPPORTED_TYPES = frozenset({"int", "long", "double", "string", "boolean"})


def validate_type(type_name: str) -> str:
    """Validate a type name, returning its canonical form."""
    if not isinstance(type_name, str) or type_name not in SUPPORTED_TYPES:
        raise ValidationError(
            f"unsupported column type: {type_name!r}",
            field="type",
            allowed=sorted(SUPPORTED_TYPES),
        )
    return type_name


def can_widen(old: str, new: str) -> bool:
    """True if ``old`` may safely evolve to ``new`` without silent truncation.

    Equality always works. Numeric widenings follow the
    ``int`` -> ``long`` -> ``double`` chain. Cross-family changes
    (e.g. numeric -> string) are *not* considered safe widenings because
    they change comparison and partitioning semantics.
    """
    if old == new:
        return True
    if old in _NUMERIC_ORDER and new in _NUMERIC_ORDER:
        return _NUMERIC_ORDER[new] >= _NUMERIC_ORDER[old]
    return False


def coerce_value(value: object, type_name: str) -> object:
    """Coerce a JSON value to the given type, raising on data loss.

    ``None`` passes through (nullability is checked by the schema layer).
    Booleans are intentionally not accepted as integers and vice versa.
    """
    validate_type(type_name)
    if value is None:
        return None
    if type_name == "int":
        if type(value) is int:
            return value
        raise ValidationError(f"value {value!r} is not an int", type_name=type_name)
    if type_name == "long":
        if type(value) is int:
            return value
        raise ValidationError(f"value {value!r} is not a long", type_name=type_name)
    if type_name == "double":
        # bool is a subclass of int in Python; reject it explicitly.
        if type(value) in (int, float) and not isinstance(value, bool):
            return float(value)
        raise ValidationError(f"value {value!r} is not a double", type_name=type_name)
    if type_name == "string":
        if type(value) is str:
            return value
        raise ValidationError(f"value {value!r} is not a string", type_name=type_name)
    if type_name == "boolean":
        if type(value) is bool:
            return value
        raise ValidationError(f"value {value!r} is not a boolean", type_name=type_name)
    raise ValidationError(f"unsupported type {type_name!r}")  # pragma: no cover


def values_equal(a: object, b: object, type_name: str) -> bool:
    """Compare two JSON-decoded values under the column type."""
    if a is None or b is None:
        return a is None and b is None
    if type_name in _NUMERIC_ORDER:
        if type(a) in (int, float) and type(b) in (int, float) \
                and not isinstance(a, bool) and not isinstance(b, bool):
            return float(a) == float(b)
        return False
    return a == b
