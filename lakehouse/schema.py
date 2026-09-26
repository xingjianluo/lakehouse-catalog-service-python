"""表结构演进与兼容性判定。

兼容规则（仅允许追加式演进）：
- 新增列：允许，但必须携带默认值（has_default=True），
  历史数据读取时按默认值回填，语义确定。
- 类型放宽（widening）：允许，如 int -> long -> double、float -> double。
- 类型收窄或跨族变更：拒绝（schema_type_narrowing）。
- 删除列：拒绝（schema_column_dropped），避免历史数据静默错位。
- 改名：拒绝（schema_column_renamed），按"同位置不同名"或
  "删旧列+加新列"的组合特征检测。
"""

from __future__ import annotations

from .errors import (
    SchemaColumnDroppedError,
    SchemaColumnRenamedError,
    SchemaDefaultMissingError,
    SchemaNarrowingError,
)
from .models import TYPE_WIDENING, Schema


def check_compatible(old: Schema, new: Schema) -> None:
    """校验 new 相对 old 是否为兼容演进，不兼容时抛出可区分错误。"""
    old_names = old.column_names()
    new_names = new.column_names()

    # 1) 删列检测：旧列名在新结构中消失。
    dropped = [n for n in old_names if n not in new_names]
    added = [n for n in new_names if n not in old_names]

    if dropped:
        # 改名检测：列数不变、仅一处差异、且类型一致 -> 视为改名。
        if len(dropped) == 1 and len(added) == 1 and len(old_names) == len(new_names):
            old_col = old.get(dropped[0])
            new_col = new.get(added[0])
            if old_col is not None and new_col is not None and old_col.type == new_col.type:
                raise SchemaColumnRenamedError(
                    f"检测到列改名: {dropped[0]!r} -> {added[0]!r}，改名会导致历史数据静默错位",
                    detail={"old": dropped[0], "new": added[0]},
                )
        raise SchemaColumnDroppedError(
            f"不允许删除列: {dropped}，删列会导致历史数据静默错位",
            detail={"dropped": dropped},
        )

    # 2) 类型检测：保留列的类型必须相同或为放宽。
    for name in old_names:
        old_col = old.get(name)
        new_col = new.get(name)
        assert old_col is not None and new_col is not None
        if old_col.type == new_col.type:
            continue
        if new_col.type in TYPE_WIDENING.get(old_col.type, set()):
            continue
        raise SchemaNarrowingError(
            f"列 {name!r} 类型变更 {old_col.type} -> {new_col.type} 为收窄或跨族变更，被拒绝",
            detail={"column": name, "from": old_col.type, "to": new_col.type},
        )

    # 3) 新增列必须携带默认值，保证历史数据回填语义确定。
    for name in added:
        col = new.get(name)
        assert col is not None
        if not col.has_default:
            raise SchemaDefaultMissingError(
                f"新增列 {name!r} 缺少默认值，无法确定历史数据回填语义",
                detail={"column": name},
            )


def backfill_row(row: dict, schema: Schema) -> dict:
    """按 schema 默认值回填缺失字段，返回新 dict（不改原行）。"""
    out = dict(row)
    for col in schema.columns:
        if col.name not in out and col.has_default:
            out[col.name] = col.default
    return out
