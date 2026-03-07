from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


import json
import os
import re
import shutil
from pathlib import Path
from typing import Any

from peter.db.repositories.site_repo import SiteRepository
from peter.db.repositories.spec_repo import SpecRepository, SpecRow
from peter.domain.errors import ValidationError
from peter.parsing.pdf_text import extract_pdf_text, PdfTextExtractionError
from peter.storage.filestore import ensure_site_folders
from peter.storage.isolation import SiteSandbox
from peter.util.hashing import sha256_file
from peter.knowledge.checklist_builder import build_decorative_checklist


class SpecService:
    """Milestone M2: spec ingestion."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings
        self.site_repo = SiteRepository(conn)
        self.spec_repo = SpecRepository(conn)

    def _validate_version(self, version_label: str) -> str:
        v = (version_label or "").strip().upper().replace(" ", "")

        # Accept both REV* and V* forms and normalize to REV*.
        # Examples: V1 -> REV1, V01 -> REV01
        m_v = re.fullmatch(r"V([0-9A-Z]{1,8})", v)
        if m_v:
            return "REV" + m_v.group(1)

        # Accept REV01, REV1, REVA, etc
        if re.fullmatch(r"REV[0-9A-Z]{1,8}", v):
            return v

        raise ValidationError("version_label must look like V1 / REV01 / REV1 / REVA")

    def ingest_spec(self, *, site_code: str, version_label: str, file_path: Path) -> SpecRow:
        site_code = (site_code or "").strip().upper()
        vlabel = self._validate_version(version_label)
        path = Path(file_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValidationError(f"Spec file not found: {path}")
        if path.suffix.lower() != ".pdf":
            raise ValidationError("Spec must be a PDF")

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

        sha = sha256_file(path)
        existing = self.spec_repo.get_by_site_sha(site.id, sha)
        if existing:
            # Idempotent: ensure derived artifacts exist (best-effort).
            # IMPORTANT: prefer SPEC_PACK as source of truth for PRODUCTS + whitelist confirmation.
            try:
                sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
                admin_dir = sandbox.build_path("00_admin")

                # Load extracted spec text if present
                txt_name_glob = f"{site.site_code}__SPEC__{vlabel}__{sha[:12]}*.txt"
                txt_files = list(admin_dir.glob(txt_name_glob))
                spec_text = txt_files[0].read_text(encoding="utf-8", errors="replace") if txt_files else ""

                products_name = f"{site.site_code}__PRODUCTS__{vlabel}__{sha[:12]}.json"
                products_path = sandbox.build_path("00_admin", products_name)

                # Try to (re)build PRODUCTS from SPEC_PACK if available
                paint_products: list[dict[str, Any]] = []
                pack_name = f"{site.site_code}__SPEC_PACK__{vlabel}__{sha[:12]}.json"
                pack_path = sandbox.build_path("00_admin", pack_name)
                if pack_path.exists():
                    pack_payload = json.loads(pack_path.read_text(encoding="utf-8"))
                    for ap in (pack_payload.get("allowed_products") or []):
                        code = ap.get("code")
                        name = ap.get("name")
                        aliases = ap.get("aliases") or []
                        brand = ap.get("brand")
                        role = ap.get("role")
                        paint_products.append(
                            {
                                "raw_mention": f"{name} ({code})" if code else str(name or ""),
                                "brand": brand,
                                "product": name,
                                "code": code,
                                "kind": "SPEC_PACK",
                                "role": role,
                                "aliases": aliases,
                            }
                        )
                    # Always overwrite so we don't get stuck with an empty legacy file.
                    products_path.write_text(
                        json.dumps(
                            {
                                "site_code": site.site_code,
                                "version": vlabel,
                                "sha256": sha,
                                "paint_products": paint_products,
                                "notes": "Derived from SPEC_PACK.allowed_products (preferred source of truth).",
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                else:
                    # Fallback: legacy heuristic extraction (only if PRODUCTS missing)
                    if spec_text and not products_path.exists():
                        from peter.knowledge.spec_products import extract_allowed_products

                        use_openai = os.getenv("PETER_SPEC_PRODUCTS_USE_OPENAI", "1").strip().lower() in ("1", "true", "yes")
                        model = os.getenv("PETER_SPEC_PRODUCTS_MODEL", "gpt-4.1")
                        products = extract_allowed_products(spec_text=spec_text, use_openai=use_openai, model=model)
                        paint_products = [p.__dict__ for p in products if p.kind == "PAINT"]
                        products_path.write_text(
                            json.dumps(
                                {
                                    "site_code": site.site_code,
                                    "version": vlabel,
                                    "sha256": sha,
                                    "paint_products": paint_products,
                                    "notes": "Generated from spec text; strict allowlist (paint only).",
                                },
                                indent=2,
                                ensure_ascii=False,
                            )
                            + "\n",
                            encoding="utf-8",
                        )

                # Ensure whitelist confirmation draft exists (built from current PRODUCTS list)
                try:
                    if not paint_products and products_path.exists():
                        try:
                            pdata = json.loads(products_path.read_text(encoding="utf-8"))
                            paint_products = pdata.get("paint_products") or []
                        except Exception:
                            paint_products = []

                    email_name = f"{site.site_code}__WHITELIST_CONFIRMATION__{vlabel}__{sha[:12]}.txt"
                    email_path = sandbox.build_path("00_admin", email_name)
                    if not email_path.exists():
                        lines: list[str] = []
                        lines.append(f"ACTION REQUIRED: Confirm extracted product whitelist — {site.site_code} {vlabel}")
                        lines.append("")
                        lines.append(f"Site: {site.site_code}")
                        lines.append(f"Spec revision: {vlabel}")
                        lines.append(f"Spec hash: {sha}")
                        lines.append("")
                        lines.append("Extracted allowed products (global union):")
                        def _role_key(x: dict[str, Any]) -> str:
                            return str(x.get("role") or "").strip().lower()
                        def _code_key(x: dict[str, Any]) -> str:
                            return str(x.get("code") or "").strip().upper()
                        def _name_key(x: dict[str, Any]) -> str:
                            return str(x.get("product") or x.get("name") or "").strip()
                        role_order = {"primer": 10, "undercoat": 20, "intermediate": 30, "topcoat": 40, "finish": 40, "surface conditioner": 90}
                        for it in sorted(paint_products, key=lambda x: (role_order.get(_role_key(x), 50), _role_key(x), _code_key(x), _name_key(x))):
                            code = (it.get("code") or "").strip()
                            name = (it.get("product") or it.get("name") or "").strip()
                            role = (it.get("role") or "").strip()
                            aliases = it.get("aliases") or []
                            if code:
                                base = f"- {code} — {name}" + (f" [{role}]" if role else "")
                            else:
                                base = f"- {name}" + (f" [{role}]" if role else "")
                            if aliases:
                                base += f" (aliases: {', '.join(str(a) for a in aliases)})"
                            lines.append(base)
                        lines.append("")
                        lines.append("Reply with: CONFIRM, or list changes (REMOVE / ADD / ADD ALIAS).")
                        email_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                except Exception:
                    pass
            except Exception:
                pass
            return existing

        # Store PDF under 01_spec with enforced naming
        safe_filename = f"{site.site_code}__SPEC__{vlabel}__{sha[:12]}.pdf"
        stored_pdf = sandbox.build_path("01_spec", safe_filename)
        shutil.copy2(path, stored_pdf)

        # Try to extract text (may fail due to missing deps)
        extracted_text_rel: str | None = None
        checklist_rel: str | None = None
        spec_text: str = ""
        try:
            spec_text = extract_pdf_text(stored_pdf)
            extracted_name = f"{site.site_code}__SPEC__{vlabel}__{sha[:12]}.txt"
            extracted_path = sandbox.build_path("00_admin", extracted_name)
            extracted_path.write_text(spec_text, encoding="utf-8")
            extracted_text_rel = str(extracted_path.relative_to(self.settings.QA_ROOT))

            # Ensure a spec_rules.json exists (editable by humans)
            try:
                from peter.analysis.spec_rules import write_default

                rules_path = sandbox.build_path("00_admin", f"{site.site_code}__SPEC_RULES__{vlabel}__{sha[:12]}.json")
                if not rules_path.exists():
                    write_default(rules_path)
            except Exception:
                pass

            checklist = build_decorative_checklist(spec_text)
            checklist_name = f"{site.site_code}__CHECKLIST__{vlabel}__{sha[:12]}.json"
            checklist_path = sandbox.build_path("00_admin", checklist_name)
            checklist_path.write_text(json.dumps(checklist, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            checklist_rel = str(checklist_path.relative_to(self.settings.QA_ROOT))

            # Build a paint product allowlist (strict, no substitutions by default)
            try:
                from peter.knowledge.spec_products import extract_allowed_products

                use_openai = os.getenv("PETER_SPEC_PRODUCTS_USE_OPENAI", "1").strip().lower() in ("1", "true", "yes")
                model = os.getenv("PETER_SPEC_PRODUCTS_MODEL", "gpt-4.1")
                products = extract_allowed_products(spec_text=spec_text, use_openai=use_openai, model=model)

                products_name = f"{site.site_code}__PRODUCTS__{vlabel}__{sha[:12]}.json"
                products_path = sandbox.build_path("00_admin", products_name)

                # Build a richer spec pack (spec type + product roles + role rules)
                pack_payload: dict[str, Any] | None = None
                try:
                    if os.getenv("PETER_SPEC_PACK_ENABLED", "1").strip().lower() in ("1", "true", "yes"):
                        from peter.knowledge.spec_pack import extract_spec_pack

                        pack = extract_spec_pack(spec_text=spec_text)
                        pack_payload = {
                            "site_code": site.site_code,
                            "version": vlabel,
                            "sha256": sha,
                            "spec_type": pack.spec_type,
                            "supplier_prefix": pack.supplier_prefix,
                            "allowed_products": pack.allowed_products,
                            "role_rules": pack.role_rules,
                        }
                        pack_name = f"{site.site_code}__SPEC_PACK__{vlabel}__{sha[:12]}.json"
                        pack_path = sandbox.build_path("00_admin", pack_name)
                        pack_path.write_text(json.dumps(pack_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
                except Exception:
                    pack_payload = None

                # Write product allowlist used by report checks.
                # IMPORTANT: prefer SPEC_PACK.allowed_products (roles + codes) as source of truth.
                paint_products: list[dict[str, Any]] = []

                if pack_payload and (pack_payload.get("allowed_products") or []):
                    for ap in (pack_payload.get("allowed_products") or []):
                        code = ap.get("code")
                        name = ap.get("name")
                        aliases = ap.get("aliases") or []
                        brand = ap.get("brand")
                        role = ap.get("role")
                        # Keep the on-disk schema compatible with ProductAllowlist.load_allowlist
                        paint_products.append(
                            {
                                "raw_mention": f"{name} ({code})" if code else str(name or ""),
                                "brand": brand,
                                "product": name,
                                "code": code,
                                "kind": "SPEC_PACK",  # informational only
                                "role": role,
                                "aliases": aliases,
                            }
                        )

                    products_path.write_text(
                        json.dumps(
                            {
                                "site_code": site.site_code,
                                "version": vlabel,
                                "sha256": sha,
                                "paint_products": paint_products,
                                "notes": "Derived from SPEC_PACK.allowed_products (preferred source of truth).",
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                else:
                    # Fallback: legacy heuristic extraction (best-effort)
                    paint_products = [p.__dict__ for p in products if p.kind == "PAINT"]
                    products_path.write_text(
                        json.dumps(
                            {
                                "site_code": site.site_code,
                                "version": vlabel,
                                "sha256": sha,
                                "paint_products": paint_products,
                                "notes": "Generated from spec text; strict allowlist (paint only).",
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n",
                        encoding="utf-8",
                    )

                # Produce a technician-facing confirmation email draft (not sent automatically).
                try:
                    lines: list[str] = []
                    lines.append(f"ACTION REQUIRED: Confirm extracted product whitelist — {site.site_code} {vlabel}")
                    lines.append("")
                    lines.append(f"Site: {site.site_code}")
                    lines.append(f"Spec revision: {vlabel}")
                    lines.append(f"Spec hash: {sha}")
                    lines.append("")
                    lines.append("Extracted allowed products (global union):")
                    # Stable sort: by role then code/name
                    def _role_key(x: dict[str, Any]) -> str:
                        return str(x.get("role") or "").strip().lower()

                    def _code_key(x: dict[str, Any]) -> str:
                        return str(x.get("code") or "").strip().upper()

                    def _name_key(x: dict[str, Any]) -> str:
                        return str(x.get("product") or x.get("name") or "").strip()

                    role_order = {"primer": 10, "undercoat": 20, "intermediate": 30, "topcoat": 40, "finish": 40, "surface conditioner": 90}

                    for it in sorted(paint_products, key=lambda x: (role_order.get(_role_key(x), 50), _role_key(x), _code_key(x), _name_key(x))):
                        code = (it.get("code") or "").strip()
                        name = (it.get("product") or it.get("name") or "").strip()
                        role = (it.get("role") or "").strip()
                        aliases = it.get("aliases") or []
                        if code:
                            base = f"- {code} — {name}" + (f" [{role}]" if role else "")
                        else:
                            base = f"- {name}" + (f" [{role}]" if role else "")
                        if aliases:
                            base += f" (aliases: {', '.join(str(a) for a in aliases)})"
                        lines.append(base)

                    lines.append("")
                    lines.append("Reply with: CONFIRM, or list changes (REMOVE / ADD / ADD ALIAS).")

                    email_name = f"{site.site_code}__WHITELIST_CONFIRMATION__{vlabel}__{sha[:12]}.txt"
                    email_path = sandbox.build_path("00_admin", email_name)
                    email_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                except Exception:
                    pass

                # Background prefetch: enqueue TDS fetches for new products (by code if available).
                try:
                    from peter.knowledge.tds_queue import enqueue

                    if os.getenv("PETER_TDS_PREFETCH_ENABLED", "").strip().lower() in ("1", "true", "yes"):
                        for pr in products:
                            if pr.kind != "PAINT":
                                continue
                            product_key = (pr.code or pr.product).strip().upper()
                            vendor = (pr.brand or "UNKNOWN").strip().upper()
                            enqueue(
                                data_dir=self.settings.DATA_DIR,
                                vendor=vendor,
                                product_key=product_key,
                                hints={"site": site.site_code, "spec_version": vlabel},
                            )
                except Exception:
                    pass

            except Exception:
                pass
        except PdfTextExtractionError:
            # Spec still ingested, but without extracted text/checklist.
            pass

        # Activate this spec: deactivate all others first (site-scoped)
        self.spec_repo.deactivate_all_for_site(site.id)

        stored_rel = str(stored_pdf.relative_to(self.settings.QA_ROOT))
        spec = self.spec_repo.insert(
            site_id=site.id,
            version_label=vlabel,
            filename=safe_filename,
            sha256=sha,
            stored_path=stored_rel,
            extracted_text_path=extracted_text_rel,
            checklist_json_path=checklist_rel,
            is_active=True,
        )
        self.spec_repo.set_site_active_spec(site.id, spec.id)
        return spec
