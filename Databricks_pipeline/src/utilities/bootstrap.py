"""
Portable Databricks notebook bootstrap.

Discovers the project root without hardcoded repo / user / workspace paths,
refreshes cached Python modules so source edits apply without a cluster restart,
and provides a single entrypoint used by every notebook.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence


PROJECT_MARKER = ("config", "config.py")
PACKAGE_PREFIXES = ("config", "src", "scripts")


def _looks_like_project_root(path: Path) -> bool:
    try:
        return (path / PROJECT_MARKER[0] / PROJECT_MARKER[1]).exists()
    except Exception:
        return False


def _walk_parents(start: Path, max_depth: int = 12) -> list[Path]:
    out: list[Path] = []
    cur = start
    for _ in range(max_depth):
        out.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    return out


def _notebook_workspace_path(dbutils: Any = None) -> Optional[Path]:
    """Resolve the absolute /Workspace/... path of the current notebook when possible."""
    try:
        if dbutils is None:
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils")  # type: ignore[union-attr]
        if dbutils is None:
            return None
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if not nb:
            return None
        nb_path = Path(str(nb))
        if str(nb_path).startswith("/Workspace"):
            return nb_path
        return Path("/Workspace") / str(nb).lstrip("/")
    except Exception:
        return None


def discover_project_root(
    dbutils: Any = None,
    extra_candidates: Optional[Sequence[str | Path]] = None,
) -> Path:
    """
    Dynamically locate the Databricks_pipeline project root.

    Search order (no hardcoded repo / username assumptions):
      1. Explicit ECOMMERCE_LAKEHOUSE_ROOT env
      2. Current working directory and parents
      3. Notebook path parents under /Workspace
      4. Shallow scan of /Workspace/Users/*/ and /Workspace/Repos/ for the marker
      5. Optional caller-provided extra candidates
    """
    import os

    env_root = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
    candidates: list[Path] = []
    if env_root:
        candidates.append(Path(env_root))

    try:
        candidates.extend(_walk_parents(Path.cwd()))
    except Exception:
        pass

    nb_path = _notebook_workspace_path(dbutils)
    if nb_path is not None:
        candidates.extend(_walk_parents(nb_path))
        try:
            candidates.extend(_walk_parents(nb_path.parent))
        except Exception:
            pass

    for base_name in ("/Workspace/Users", "/Workspace/Repos", "/Workspace"):
        base = Path(base_name)
        if not base.exists():
            continue
        try:
            candidates.append(base)
            for child in list(base.iterdir())[:80]:
                if not child.is_dir():
                    continue
                candidates.append(child)
                try:
                    for grandchild in list(child.iterdir())[:40]:
                        if grandchild.is_dir():
                            candidates.append(grandchild)
                except Exception:
                    pass
        except Exception:
            pass

    if extra_candidates:
        candidates.extend(Path(p) for p in extra_candidates)

    seen: set[str] = set()
    for cand in candidates:
        try:
            resolved = str(cand.resolve()) if cand.exists() else str(cand)
        except Exception:
            resolved = str(cand)
        if resolved in seen:
            continue
        seen.add(resolved)
        if _looks_like_project_root(cand):
            return cand

    raise FileNotFoundError(
        "Could not locate Databricks_pipeline project root "
        f"(marker {PROJECT_MARKER[0]}/{PROJECT_MARKER[1]}). "
        "Set ECOMMERCE_LAKEHOUSE_ROOT or open the notebook from inside the repo."
    )


def ensure_project_on_syspath(
    dbutils: Any = None,
    extra_candidates: Optional[Sequence[str | Path]] = None,
) -> Path:
    """Discover project root and insert it at the front of sys.path."""
    root = discover_project_root(dbutils=dbutils, extra_candidates=extra_candidates)
    root_str = str(root)
    if root_str in sys.path:
        sys.path.remove(root_str)
    sys.path.insert(0, root_str)
    return root


def clear_project_modules(prefixes: Iterable[str] = PACKAGE_PREFIXES) -> list[str]:
    """Drop cached project packages from sys.modules."""
    prefixes = tuple(prefixes)
    removed: list[str] = []
    for name in list(sys.modules):
        if any(name == p or name.startswith(p + ".") for p in prefixes):
            del sys.modules[name]
            removed.append(name)
    return removed


def reload_project_modules(prefixes: Iterable[str] = PACKAGE_PREFIXES) -> list[str]:
    """Clear then eagerly re-import core packages so notebooks always see latest code."""
    removed = clear_project_modules(prefixes)
    for pkg in prefixes:
        try:
            importlib.import_module(pkg)
        except Exception:
            pass
    return removed


def notebook_dir(dbutils: Any = None) -> Optional[Path]:
    """Directory containing the currently executing notebook (workspace path)."""
    nb = _notebook_workspace_path(dbutils)
    return nb.parent if nb is not None else None


def resolve_notebook_path(relative: str, dbutils: Any = None) -> str:
    """
    Resolve a notebook path for dbutils.notebook.run portably.

    Relative paths are resolved against the current notebook's directory.
    """
    rel = relative.strip()
    if rel.startswith("/"):
        return rel
    base = notebook_dir(dbutils)
    if base is None:
        return rel
    clean = rel[2:] if rel.startswith("./") else rel
    return str((base / clean).as_posix())


def bootstrap_notebook(
    dbutils: Any = None,
    reload_modules: bool = True,
    extra_candidates: Optional[Sequence[str | Path]] = None,
) -> Path:
    """
    Full notebook bootstrap: discover root → sys.path → refresh modules.

    Call this as the first executable step in every Databricks notebook.
    """
    root = ensure_project_on_syspath(dbutils=dbutils, extra_candidates=extra_candidates)
    if reload_modules:
        reload_project_modules()
        ensure_project_on_syspath(dbutils=dbutils, extra_candidates=extra_candidates)
    return root
