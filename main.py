"""FastAPI 入口：本地湖仓表目录服务（骨架）。"""

from __future__ import annotations

import tempfile
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from lakehouse.catalog import Catalog

app = FastAPI(title="lakehouse-catalog")
catalog = Catalog(Path(tempfile.mkdtemp(prefix="lakehouse-")))


@app.get("/")
def read_root():
    return {"service": "lakehouse-catalog", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
