"""本地文件存储：元数据 JSON 的原子读写与分区数据文件。

原子写语义：先写临时文件，再 os.replace 到目标路径，
保证读者任一时刻只能看到完整文件（无部分写入）。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class LocalStorage:
    """以本地目录模拟对象存储，提供原子写与读。"""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _abs(self, path: Path) -> Path:
        p = Path(path)
        return p if p.is_absolute() else self.root / p

    def read_json(self, path: Path) -> Any:
        with open(self._abs(path), "r", encoding="utf-8") as f:
            return json.load(f)

    def write_json_atomic(self, path: Path, data: Any) -> None:
        target = self._abs(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        with self._lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)

    def write_data_file(self, path: Path, rows: list[dict]) -> None:
        target = self._abs(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        with self._lock:
            with open(tmp, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)

    def read_data_file(self, path: Path) -> list[dict]:
        rows: list[dict] = []
        with open(self._abs(path), "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def exists(self, path: Path) -> bool:
        return self._abs(path).exists()

    def delete(self, path: Path) -> None:
        try:
            self._abs(path).unlink()
        except FileNotFoundError:
            pass
