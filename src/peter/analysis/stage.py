from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ReportStage:
    stage: str  # EARLY|MID|LATE|REFERENCE_PANEL|UNKNOWN
    rationale: str


def infer_stage_from_text(text: str) -> ReportStage:
    """Best-effort stage inference from extracted report text.

    Keep this conservative; it's used to adjust evidence expectations and summaries.
    """

    t = (text or "")
    low = t.lower()

    # Reference panel / sample / approval language
    if "reference panel" in low or "sample panel" in low or "approval panel" in low:
        return ReportStage("REFERENCE_PANEL", "Detected reference/sample panel language")

    # Late-stage indicators: DFT emphasis, finishes, topcoat completion
    late_hits = sum(
        1
        for k in [
            "dft readings",
            "dry film thickness",
            "final coat",
            "finish coat",
            "topcoat",
            "snag",
            "handover",
        ]
        if k in low
    )

    # Early-stage indicators: moisture/repairs/prep emphasis
    early_hits = sum(
        1
        for k in [
            "moisture",
            "plaster",
            "skim coat",
            "repairs",
            "surface preparation",
            "crack repair",
            "ready for painting",
        ]
        if k in low
    )

    if late_hits >= 3 and late_hits > early_hits:
        return ReportStage("LATE", f"Late indicators={late_hits} early={early_hits}")
    if early_hits >= 3 and early_hits >= late_hits:
        return ReportStage("EARLY", f"Early indicators={early_hits} late={late_hits}")

    if late_hits or early_hits:
        return ReportStage("MID", f"Mixed indicators early={early_hits} late={late_hits}")

    return ReportStage("UNKNOWN", "No clear stage indicators")
