from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class SiteIsolationError(RuntimeError):
    pass


@dataclass(frozen=True)
class SiteSandbox:
    """Filesystem sandbox for a single site.

    All file access for a site must be validated through this class.
    """

    site_root: Path  # absolute

    def resolve_under_root(self, candidate: Path) -> Path:
        root = self.site_root.expanduser().resolve()
        cand = candidate.expanduser().resolve()
        try:
            cand.relative_to(root)
        except ValueError as e:
            raise SiteIsolationError(f"Path escapes site root. root={root} candidate={cand}") from e
        return cand

    def build_path(self, *parts: str) -> Path:
        p = self.site_root / Path(*parts)
        return self.resolve_under_root(p)

    def ensure_dir(self, *parts: str) -> Path:
        d = self.build_path(*parts)
        d.mkdir(parents=True, exist_ok=True)
        return d
