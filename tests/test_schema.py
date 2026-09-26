"""结构演进兼容性判定测试：失败按 reason 区分，日志打印判定依据。"""

import logging

import pytest

from lakehouse.errors import (
    SchemaColumnDroppedError,
    SchemaColumnRenamedError,
    SchemaDefaultMissingError,
    SchemaNarrowingError,
)
from lakehouse.models import Column, Schema
from lakehouse.schema import backfill_row, check_compatible

log = logging.getLogger("lakehouse.tests.schema")

BASE = Schema((Column("id", "long"), Column("v", "int")))


def test_add_column_with_default_is_compatible():
    new = Schema(BASE.columns + (Column("tag", "string", default="n/a", has_default=True),))
    check_compatible(BASE, new)
    log.info("判定通过: 新增列 tag 带默认值 'n/a'，历史行将回填该默认值")
    assert backfill_row({"id": 1, "v": 2}, new) == {"id": 1, "v": 2, "tag": "n/a"}


def test_type_widening_is_compatible():
    new = Schema((Column("id", "long"), Column("v", "double")))
    check_compatible(BASE, new)
    log.info("判定通过: v 类型 int -> double 为放宽（widening）")


@pytest.mark.parametrize("old_type,new_type", [
    ("long", "int"), ("double", "int"), ("string", "long"), ("int", "string"),
])
def test_type_narrowing_rejected(old_type, new_type):
    old = Schema((Column("id", "long"), Column("v", old_type)))
    new = Schema((Column("id", "long"), Column("v", new_type)))
    with pytest.raises(SchemaNarrowingError) as exc:
        check_compatible(old, new)
    log.info("判定拒绝: reason=%s 列 v 类型 %s -> %s 为收窄/跨族变更",
             exc.value.reason, old_type, new_type)
    assert exc.value.reason == "schema_type_narrowing"


def test_drop_column_rejected():
    new = Schema((Column("id", "long"),))
    with pytest.raises(SchemaColumnDroppedError) as exc:
        check_compatible(BASE, new)
    log.info("判定拒绝: reason=%s 删除列 %s 会导致历史数据静默错位",
             exc.value.reason, exc.value.detail["dropped"])
    assert exc.value.reason == "schema_column_dropped"


def test_rename_column_rejected():
    new = Schema((Column("id", "long"), Column("w", "int")))
    with pytest.raises(SchemaColumnRenamedError) as exc:
        check_compatible(BASE, new)
    log.info("判定拒绝: reason=%s 改名 %s -> %s 会导致历史数据静默错位",
             exc.value.reason, exc.value.detail["old"], exc.value.detail["new"])
    assert exc.value.reason == "schema_column_renamed"


def test_add_column_without_default_rejected():
    new = Schema(BASE.columns + (Column("x", "string"),))
    with pytest.raises(SchemaDefaultMissingError) as exc:
        check_compatible(BASE, new)
    log.info("判定拒绝: reason=%s 新增列 %s 缺少默认值，回填语义不确定",
             exc.value.reason, exc.value.detail["column"])
    assert exc.value.reason == "schema_default_missing"
