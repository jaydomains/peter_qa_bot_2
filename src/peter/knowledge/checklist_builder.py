from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChecklistItem:
    key: str
    label: str
    required: bool = True


def build_decorative_checklist(spec_text: str) -> dict[str, Any]:
    """Build a structured checklist JSON.

    v0: deterministic baseline checklist (not spec-specific yet).
    Later: parse spec_text into clauses and map into checklist items.
    """

    items = [
        ChecklistItem("moisture_confirmation", "Moisture confirmation recorded"),
        ChecklistItem("ambient_temp", "Ambient temperature recorded"),
        ChecklistItem("substrate_temp", "Substrate temperature recorded"),
        ChecklistItem("relative_humidity", "Relative humidity recorded"),
        ChecklistItem("dew_point", "Dew point considered/recorded"),
        ChecklistItem("cleaning_method", "Cleaning/surface prep method described"),
        ChecklistItem("primer", "Primer specified and recorded"),
        ChecklistItem("coat_count", "Coat count recorded"),
        ChecklistItem("batch_numbers", "Batch numbers recorded"),
        ChecklistItem("mixing", "Mixing documentation recorded"),
        ChecklistItem("recoat_window", "Recoat windows recorded"),
        ChecklistItem("curing_time", "Curing time / protection recorded"),
    ]

    return {
        "schema_version": "checklist_v0",
        "items": [item.__dict__ for item in items],
        "notes": "Baseline decorative coatings checklist; extend with spec clause extraction in later versions.",
    }
