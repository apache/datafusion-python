"""Ensure exposed pyclasses default to frozen."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

PYCLASS_RE = re.compile(
    r"#\[\s*pyclass\s*(?:\((?P<args>.*?)\))?\s*\]",
    re.DOTALL,
)
ARG_STRING_RE = re.compile(
    r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*\"(?P<value>[^\"]+)\"",
)
STRUCT_NAME_RE = re.compile(
    r"\b(?:pub\s+)?(?:struct|enum)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)",
)


@dataclass
class PyClass:
    module: str
    name: str
    frozen: bool
    source: Path


def iter_pyclasses(root: Path) -> Iterator[PyClass]:
    for path in root.rglob("*.rs"):
        text = path.read_text(encoding="utf8")
        for match in PYCLASS_RE.finditer(text):
            args = match.group("args") or ""
            frozen = re.search(r"\bfrozen\b", args) is not None

            module = None
            name = None
            for arg_match in ARG_STRING_RE.finditer(args):
                key = arg_match.group("key")
                value = arg_match.group("value")
                if key == "module":
                    module = value
                elif key == "name":
                    name = value

            remainder = text[match.end() :]
            struct_match = STRUCT_NAME_RE.search(remainder)
            struct_name = struct_match.group("name") if struct_match else None

            yield PyClass(
                module=module or "datafusion",
                name=name or struct_name or "<unknown>",
                frozen=frozen,
                source=path,
            )


def test_pyclasses_are_frozen() -> None:
    allowlist = {
        # NOTE: Any new exceptions must include a justification comment
        # in the Rust source and, ideally, a follow-up issue to remove
        # the exemption.
        ("datafusion.common", "SqlTable"),
        ("datafusion.common", "SqlView"),
        ("datafusion.common", "DataTypeMap"),
        ("datafusion.expr", "TryCast"),
        ("datafusion.expr", "WriteOp"),
    }

    unfrozen = [
        pyclass
        for pyclass in iter_pyclasses(Path("src"))
        if not pyclass.frozen and (pyclass.module, pyclass.name) not in allowlist
    ]

    if unfrozen:
        msg = (
            "Found pyclasses missing `frozen`; add them to the allowlist only "
            "with a justification comment and follow-up plan:\n"
        )
        msg += "\n".join(
            (f"- {pyclass.module}.{pyclass.name} (defined in {pyclass.source})")
            for pyclass in unfrozen
        )
        assert not unfrozen, msg
