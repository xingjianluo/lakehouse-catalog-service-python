"""Local development entry point: ``python main.py``."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import uvicorn

from lakehouse_catalog.api import create_app

warehouse = os.environ.get(
    "LAKEHOUSE_WAREHOUSE",
    str(Path(tempfile.gettempdir()) / "lakehouse_warehouse"),
)
app = create_app(warehouse)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
