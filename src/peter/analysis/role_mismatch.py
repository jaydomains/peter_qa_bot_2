from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RoleMismatchFinding:
    severity: str  # CRITICAL|MAJOR|MINOR|INFO
    title: str
    details: str
    requires_confirmation: bool = True


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def load_spec_pack(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and (data.get("allowed_products") or data.get("role_rules")):
            return data
    except Exception:
        return None
    return None


def detect_role_mismatches(*, spec_pack: dict[str, Any], report_text: str) -> list[RoleMismatchFinding]:
    """Heuristic role-mismatch detection.

    This is NOT trying to prove product application, only to catch situations where:
    - the spec contains constraints about PATCH/SPOT priming vs FULL COAT priming
    - the report text indicates FULL COAT / PRIMER COAT across broad areas

    We flag as requires_confirmation unless the report text is explicit.
    """

    low = (report_text or "").lower()
    rules = spec_pack.get("role_rules") or []

    # If the spec has any patch-prime threshold language, enable the check.
    has_patch_threshold = False
    for r in rules:
        if not isinstance(r, dict):
            continue
        txt = str(r.get("text") or "").lower()
        if "patch prim" in txt and ("50%" in txt or "50 %" in txt or "more of the surface" in txt):
            has_patch_threshold = True
            break

    if not has_patch_threshold:
        return []

    # Evidence in report text
    mentions_spot = any(k in low for k in ["spot prime", "spot-prim", "patch prime", "patch-prim", "touch up", "touch-up"])
    mentions_full = any(
        k in low
        for k in [
            "full coat",
            "entire",
            "whole building",
            "all areas",
            "primer coat",
            "primed",
            "used as primer",
        ]
    )

    # If report explicitly says full primer was applied AND spec contains patch threshold rule,
    # flag it as a potential system-role mismatch that should be confirmed.
    if mentions_full and not mentions_spot:
        return [
            RoleMismatchFinding(
                severity="MAJOR",
                title="Potential coating system role mismatch (patch prime vs full coat primer)",
                details=(
                    "Spec includes a patch-priming threshold rule (e.g. if patch priming covers ~50%+ then a full coat primer is required/recommended). "
                    "Report text suggests broad primer usage (full coat/primer coat/entire areas). "
                    "This may be correct, but it should be confirmed because misusing a spot/patch product as a full primer can cause failures."
                ),
                requires_confirmation=True,
            )
        ]

    return []
