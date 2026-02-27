from __future__ import annotations

from pathlib import Path

from peter.config.settings import Settings
from peter.storage.isolation import SiteSandbox
from peter.storage.paths import site_root


SITE_SUBDIRS = (
    "00_admin",
    "01_spec",
    "02_reports",
    "03_reviews",
    "04_email_archive",
    "05_feedback",
    "99_quarantine",
)


def ensure_site_folders(settings: Settings, *, folder_name: str) -> SiteSandbox:
    root = site_root(settings, folder_name)
    root.mkdir(parents=True, exist_ok=True)
    sandbox = SiteSandbox(site_root=root)
    for d in SITE_SUBDIRS:
        sandbox.ensure_dir(d)
    return sandbox
