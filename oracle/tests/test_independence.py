"""The oracle shares nothing with forseti-core and the cli: it imports only itself, rfc8785, jsonschema and the standard library, no
module launches a process, and the Java side launches a process only from the gate (to compare). The two sides meet at data files."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "oracle" / "src" / "forseti_oracle"
ALLOWED = {"forseti_oracle", "rfc8785", "jsonschema"}
PROCESS_CALLS = {("os", "system"), ("os", "popen"), ("os", "execv"), ("os", "execvp"), ("os", "spawnv"), ("os", "fork"), ("os", "startfile")}
JAVA_SOURCE = re.compile(r"[\"'](?:core|cli)/")


def modules():
    return sorted(PACKAGE.glob("*.py"))


def imports(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module.split(".")[0]


def test_the_oracle_imports_only_itself_rfc8785_jsonschema_and_the_standard_library():
    assert len(modules()) >= 9
    for f in modules():
        for name in imports(ast.parse(f.read_text(encoding="utf-8"))):
            assert name in ALLOWED or name in sys.stdlib_module_names, f"{f.name} imports {name}"


def test_no_oracle_module_launches_a_process():
    for f in modules():
        tree = ast.parse(f.read_text(encoding="utf-8"))
        assert not set(imports(tree)) & {"subprocess", "multiprocessing", "pty", "ctypes"}, f.name
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                assert (node.value.id, node.attr) not in PROCESS_CALLS, f"{f.name}: {node.value.id}.{node.attr}"


def test_the_oracle_reads_no_java_source_or_build_output():
    for f in modules():
        text = f.read_text(encoding="utf-8")
        assert not JAVA_SOURCE.search(text), f.name
        assert ".jar" not in text, f.name


def test_java_launches_a_process_only_from_the_gate():
    hits = []
    for f in list((ROOT / "core" / "src" / "main").rglob("*.java")) + list((ROOT / "cli" / "src" / "main").rglob("*.java")):
        text = f.read_text(encoding="utf-8")
        if "ProcessBuilder" in text or "Runtime.getRuntime().exec" in text:
            hits.append(f.relative_to(ROOT).as_posix())
    assert all("/cli/gate/" in h for h in hits), hits


def test_the_shared_files_are_data():
    """What both sides read: the schemas and the fixtures (registry, recordings, scenarios, the vector file); none of it is code."""
    for d in (ROOT / "schemas", ROOT / "fixtures"):
        for f in d.rglob("*"):
            if f.is_file():
                assert f.suffix not in (".py", ".pyc", ".java", ".class", ".jar", ".sh", ".ps1", ".js", ".kt"), f
