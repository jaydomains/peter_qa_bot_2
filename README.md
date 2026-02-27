# PETER (Decorative Architectural Coatings QA System)

Production-grade QA automation system for **Decorative Architectural Coatings only**.

## Status
Phase 1 (CLI) scaffold + M1 implemented:
- SQLite database initialization (WAL + foreign keys)
- Site creation + strict per-site folder sandbox
- CLI: `create-site`, `list-sites`

## Quickstart (dev)

```bash
cd qa_bot

# If you have venv support installed, install editable:
#   python3 -m venv .venv && . .venv/bin/activate && pip install -e .
# Otherwise (common on minimal Debian images), run via PYTHONPATH:

# create a site
PYTHONPATH=src python3 -m peter create-site --code WFT001 --name "Waterfront Plaza" --address ""

# list sites
PYTHONPATH=src python3 -m peter list-sites
```

Data is stored under:
- `data/qa.db`
- `data/QA_ROOT/SITES/<SITE_CODE>__<slug>/...`

## Non-negotiables
- Strict site isolation (filesystem + DB queries must always be scoped by site)
- Decorative architectural coatings scope only
- Email replies must be internal-only (Phase 2)
