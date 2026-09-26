"""表结构演进与兼容性判定（骨架）。"""

from __future__ import annotations

from .models import Schema


def check_compatible(old: Schema, new: Schema) -> None:
    """校验 new 相对 old 是否为兼容演进，不兼容时抛出可区分错误。"""
    raise NotImplementedError
