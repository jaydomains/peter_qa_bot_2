from __future__ import annotations

from enum import Enum


class Result(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
