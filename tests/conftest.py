"""Shared fixtures and logging configuration.

Logging is configured so each unit-test run prints the commit id, the
snapshot version and the decision/reason basis for accept/reject/read.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lakehouse_catalog.api import create_app
from lakehouse_catalog.catalog import LakehouseCatalog


@pytest.fixture(autouse=True)
def _catalog_logging(caplog):
    logger = logging.getLogger("lakehouse_catalog")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    caplog.set_level(logging.DEBUG, logger="lakehouse_catalog")
    yield
    logger.removeHandler(handler)


@pytest.fixture()
def warehouse(tmp_path: Path) -> Path:
    return tmp_path / "warehouse"


@pytest.fixture()
def catalog(warehouse: Path) -> LakehouseCatalog:
    # Strict OCC: commits must state base_version, so real concurrent
    # writers demonstrably conflict.
    return LakehouseCatalog(warehouse, require_explicit_base=True)


@pytest.fixture()
def client(warehouse: Path) -> TestClient:
    return TestClient(create_app(warehouse))


@pytest.fixture()
def simple_table(catalog: LakehouseCatalog) -> LakehouseCatalog:
    """A partitioned table ``events`` with columns id/msg/dt, at v1."""
    catalog.create_table(
        "events",
        [
            {"name": "id", "type": "int", "nullable": False},
            {"name": "msg", "type": "string"},
            {"name": "dt", "type": "string"},
        ],
        partition_by=["dt"],
        commit_id="create-1",
    )
    return catalog
