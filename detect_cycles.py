# -*- coding: utf-8 -*-
"""Detect circular imports in the paper_engine package via AST."""
import ast
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PKG_DIR = ROOT / "paper_engine"


def module_name_from_path(path: Path) -> str:
    rel = path.relative_to(ROOT)
    parts = rel.with_suffix("").parts
    return ".".join(parts)


def parse_imports(path: Path) -> set:
    """Return set of paper_engine.* module names imported at any scope."""
    mod = module_name_from_path(path)
    deps = set()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        print(f"[SYNTAX ERROR] {path}: {exc}", file=sys.stderr)
        return deps

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("paper_engine."):
                # normalize submodule import
                dep = node.module
                deps.add(dep)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name.startswith("paper_engine"):
                    deps.add(name)
    return deps


def find_cycles(graph):
    """Return list of simple cycles in graph (dict: node -> set(neighbors))."""
    cycles = []
    visited = set()
    stack = []

    def dfs(node):
        if node in stack:
            idx = stack.index(node)
            cycle = stack[idx:] + [node]
            cycles.append(tuple(cycle))
            return
        if node in visited:
            return
        visited.add(node)
        stack.append(node)
        for neighbor in sorted(graph.get(node, set())):
            dfs(neighbor)
        stack.pop()

    for node in sorted(graph):
        dfs(node)
    return cycles


def main():
    graph = {}
    for path in sorted(PKG_DIR.glob("*.py")):
        mod = module_name_from_path(path)
        if mod == "paper_engine":
            continue
        deps = parse_imports(path)
        # Filter to other modules in the package
        deps = {d for d in deps if d.startswith("paper_engine.") and d != mod}
        graph[mod] = deps
        print(f"{mod}: {sorted(deps)}")

    cycles = find_cycles(graph)
    if not cycles:
        print("\nNo cycles detected.")
        return 0

    print(f"\nDetected {len(cycles)} cycle(s):")
    seen = set()
    for cyc in cycles:
        key = tuple(sorted(cyc[:-1]))
        if key not in seen:
            seen.add(key)
            print(" -> ".join(cyc))
    return 1


if __name__ == "__main__":
    sys.exit(main())
