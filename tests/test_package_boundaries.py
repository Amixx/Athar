from __future__ import annotations

import ast
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
ATHAR_ROOT = REPO_ROOT / "athar"
ALLOWED_GRAPH_EXTERNAL_ROOTS = {"ifcopenshell"}


def _python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def _module_name(path: Path) -> str:
    rel = path.relative_to(REPO_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _resolved_import_name(path: Path, node: ast.ImportFrom) -> str | None:
    if node.level == 0:
        return node.module

    package_parts = list(_module_name(path).split("."))
    if path.name != "__init__.py":
        package_parts = package_parts[:-1]
    ascend = max(node.level - 1, 0)
    if ascend:
        package_parts = package_parts[:-ascend]
    if node.module:
        package_parts.extend(node.module.split("."))
    return ".".join(part for part in package_parts if part)


def _is_stdlib(root: str) -> bool:
    return root in sys.stdlib_module_names


def test_athar_package_inits_do_not_reexport_symbols():
    init_paths = sorted(ATHAR_ROOT.rglob("__init__.py"))
    assert init_paths, "expected athar package __init__.py files"
    for path in init_paths:
        module = ast.parse(path.read_text(), filename=str(path))
        body = list(module.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        assert not body, f"{path.relative_to(REPO_ROOT)} should stay a minimal package marker"


def test_athar_has_no_wildcard_imports():
    for path in _python_files(ATHAR_ROOT):
        module = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(module):
            if isinstance(node, ast.ImportFrom):
                assert all(alias.name != "*" for alias in node.names), (
                    f"wildcard import found in {path.relative_to(REPO_ROOT)}"
                )


def test_graph_package_only_imports_graph_internal_modules_and_ifcopenshell():
    graph_root = ATHAR_ROOT / "graph"
    for path in _python_files(graph_root):
        module = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(module):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0]
                    if root == "athar":
                        assert alias.name.startswith("athar.graph."), (
                            f"{path.relative_to(REPO_ROOT)} imports forbidden internal module {alias.name}"
                        )
                    else:
                        assert _is_stdlib(root) or root in ALLOWED_GRAPH_EXTERNAL_ROOTS, (
                            f"{path.relative_to(REPO_ROOT)} imports forbidden external module {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                resolved = _resolved_import_name(path, node)
                if not resolved:
                    continue
                root = resolved.split(".")[0]
                if root == "athar":
                    assert resolved == "athar.graph" or resolved.startswith("athar.graph."), (
                        f"{path.relative_to(REPO_ROOT)} imports forbidden internal module {resolved}"
                    )
                else:
                    assert _is_stdlib(root) or root in ALLOWED_GRAPH_EXTERNAL_ROOTS, (
                        f"{path.relative_to(REPO_ROOT)} imports forbidden external module {resolved}"
                    )


def test_diff_package_only_imports_diff_and_graph_internal_modules():
    diff_root = ATHAR_ROOT / "diff"
    for path in _python_files(diff_root):
        module = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(module):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if not alias.name.startswith("athar."):
                        continue
                    assert alias.name.startswith("athar.diff.") or alias.name.startswith(
                        "athar.graph."
                    ) or alias.name.startswith("athar._native."), (
                        f"{path.relative_to(REPO_ROOT)} imports forbidden internal module {alias.name}"
                    )
            elif isinstance(node, ast.ImportFrom):
                resolved = _resolved_import_name(path, node)
                if not resolved or not resolved.startswith("athar."):
                    continue
                assert (
                    resolved == "athar.graph"
                    or resolved == "athar.diff"
                    or resolved == "athar._native"
                    or resolved.startswith("athar.graph.")
                    or resolved.startswith("athar.diff.")
                    or resolved.startswith("athar._native.")
                ), f"{path.relative_to(REPO_ROOT)} imports forbidden internal module {resolved}"
