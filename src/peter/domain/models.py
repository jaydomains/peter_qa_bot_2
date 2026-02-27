from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Site:
    id: int
    site_code: str
    site_name: str
    address: str
    folder_name: str
    active_spec_id: Optional[int] = None
