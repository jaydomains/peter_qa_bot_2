from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def use_report_folders() -> bool:
    """Feature flag for Option B storage layout."""
    return os.getenv("PETER_STORAGE_REPORT_FOLDERS", "").strip().lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class Layout:
    base_dir: Path

    @staticmethod
    def from_env() -> "Layout":
        base = os.getenv("PETER_DATA_DIR", "").strip()
        if base:
            return Layout(Path(base).expanduser().resolve())
        return Layout(Path.cwd().joinpath("data").resolve())

    def sites_dir(self) -> Path:
        return self.base_dir / "sites"

    def downloads_dir(self) -> Path:
        return self.base_dir / "downloads"

    def site_root(self, site_code: str) -> Path:
        return self.sites_dir() / site_code

    def site_inbox_root(self, site_code: str) -> Path:
        return self.site_root(site_code) / "INBOX"

    def spec_inbox(self, site_code: str) -> Path:
        return self.site_inbox_root(site_code) / "specs"

    def spec_library(self, site_code: str) -> Path:
        return self.site_root(site_code) / "specs"

    def report_inbox(self, site_code: str) -> Path:
        if use_report_folders():
            return self.site_inbox_root(site_code) / "reports"
        return self.site_inbox_root(site_code)

    def report_library(self, site_code: str) -> Path:
        if use_report_folders():
            return self.site_root(site_code) / "reports"
        return self.site_root(site_code)

    def ensure_site_dirs(self, site_code: str) -> None:
        self.sites_dir().mkdir(parents=True, exist_ok=True)
        self.downloads_dir().mkdir(parents=True, exist_ok=True)

        self.site_root(site_code).mkdir(parents=True, exist_ok=True)
        self.site_inbox_root(site_code).mkdir(parents=True, exist_ok=True)
        self.spec_inbox(site_code).mkdir(parents=True, exist_ok=True)
        self.report_inbox(site_code).mkdir(parents=True, exist_ok=True)
        self.spec_library(site_code).mkdir(parents=True, exist_ok=True)
        self.report_library(site_code).mkdir(parents=True, exist_ok=True)
