import pytest

from peter.config.settings import Settings
from peter.services.spec_service import SpecService


def test_version_label_validation_rejects_bad(tmp_path):
    import sqlite3

    s = Settings.load(project_root=tmp_path)
    conn = sqlite3.connect(":memory:")
    svc = SpecService(conn, s)
    with pytest.raises(Exception):
        svc._validate_version("rev 01x!")


def test_version_label_validation_accepts():
    import sqlite3
    from pathlib import Path

    s = Settings.load(project_root=Path(__file__).resolve().parents[2])
    conn = sqlite3.connect(":memory:")
    svc = SpecService(conn, s)
    assert svc._validate_version("Rev 01") == "REV01"
