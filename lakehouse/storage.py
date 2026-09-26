"""本地文件存储：元数据 JSON 的原子读写与分区数据文件（骨架）。"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any


class LocalStorage:
    """以本地目录模拟对象存储，提供原子写与读。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._lock = threading.Lock()

    def read_json(self, path: Path) -> Any:
        raise NotImplementedError

    def write_json_atomic(self, path: Path, data: Any) -> None:
        raise NotImplementedError

    def write_data_file(self, path: Path, rows: list[dict]) -> None:
        raise NotImplementedError

    def read_data_file(self, path: Path) -> list[dict]:
        raise NotImplementedError

    def exists(self, path: Path) -> bool:
        raise NotImplementedError

    def delete(self, path: Path) -> None:
        raise NotImplementedError
