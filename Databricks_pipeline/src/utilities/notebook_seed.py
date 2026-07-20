"""
Minimal notebook seed — locate project root with ZERO package imports.

Copied into every Databricks notebook as the first Python cell so that
``src.utilities.bootstrap`` can be imported afterwards. Discovery walks
cwd + notebook parents + a shallow /Workspace scan — no hardcoded
repo names, usernames, or catalog paths.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def seed_project_root(dbutils: Any = None) -> str:
    """Find Databricks_pipeline root and insert it at sys.path[0]."""

    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()

    env = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
    candidates: list[Path] = []
    if env:
        candidates.append(Path(env))

    try:
        candidates.extend([Path.cwd(), *list(Path.cwd().parents)[:12]])
    except Exception:
        pass

    try:
        if dbutils is None:
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore[union-attr]
        if dbutils is not None:
            nb = Path(
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            ws = nb if str(nb).startswith("/Workspace") else Path("/Workspace") / str(nb).lstrip("/")
            candidates = [ws, *list(ws.parents)[:12]] + candidates
    except Exception:
        pass

    for base_name in ("/Workspace/Users", "/Workspace/Repos", "/Workspace"):
        base = Path(base_name)
        if not base.exists():
            continue
        try:
            for child in list(base.iterdir())[:80]:
                if not child.is_dir():
                    continue
                candidates.append(child)
                try:
                    for gc in list(child.iterdir())[:40]:
                        if gc.is_dir():
                            candidates.append(gc)
                except Exception:
                    pass
        except Exception:
            pass

    seen: set[str] = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        try:
            if _is_root(cand):
                root = str(cand.resolve() if cand.exists() else cand)
                if root in sys.path:
                    sys.path.remove(root)
                sys.path.insert(0, root)
                return root
        except Exception:
            continue

    raise FileNotFoundError(
        "Databricks_pipeline root not found "
        "(expected config/config.py). Set ECOMMERCE_LAKEHOUSE_ROOT."
    )


# Inline template used by notebooks (kept here as documentation / single source).
NOTEBOOK_SEED_SOURCE = r'''
import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
    if env:
        candidates.append(Path(env))
    try:
        candidates.extend([Path.cwd(), *list(Path.cwd().parents)[:12]])
    except Exception:
        pass
    try:
        nb = Path(
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook().getContext().notebookPath().get()
        )
        ws = nb if str(nb).startswith("/Workspace") else Path("/Workspace") / str(nb).lstrip("/")
        candidates = [ws, *list(ws.parents)[:12]] + candidates
    except Exception:
        pass
    for base_name in ("/Workspace/Users", "/Workspace/Repos", "/Workspace"):
        base = Path(base_name)
        if not base.exists():
            continue
        try:
            for child in list(base.iterdir())[:80]:
                if not child.is_dir():
                    continue
                candidates.append(child)
                try:
                    for gc in list(child.iterdir())[:40]:
                        if gc.is_dir():
                            candidates.append(gc)
                except Exception:
                    pass
        except Exception:
            pass
    seen = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        if _is_root(cand):
            root = str(cand)
            if root in sys.path:
                sys.path.remove(root)
            sys.path.insert(0, root)
            return root
    raise FileNotFoundError(
        "Databricks_pipeline root not found. Set ECOMMERCE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))
'''
