#!/usr/bin/env python3
"""Prevent compatibility-record persistence and fake-success paths returning."""

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def main():
    source = (REPO / "src/main.cpp").read_text(encoding="utf-8")
    header = (REPO / "src/common/FeatureGate.h").read_text(encoding="utf-8")
    session_header = (REPO / "src/utils/Session.h").read_text(encoding="utf-8")
    manual = (REPO / "docs/MANUAL.md").read_text(encoding="utf-8")

    forbidden = (
        ".pg_compat_objects",
        "CatalogObjectInfo",
        "compatObjectCatalogPath",
        "loadCompatObjects",
        "saveCompatObjects",
        "compatKindHasRuntime",
        "No compatibility objects found",
    )
    for marker in forbidden:
        assert marker not in source, "fake compatibility path returned: %s" % marker
    assert "compatKindHasRuntime" not in header
    assert "compat_object_record_layer" not in source
    assert "record layer" not in session_header
    assert "only project extensions backed by a real runtime" in session_header
    assert "恢复旧的兼容对象记录行为" not in manual
    assert "旧的兼容对象记录层及其" in manual
    assert "A runtime-backed CREATE must be consumed" in source
    assert 'featureNotSupportedError(string("CREATE ") + phrase)' in source
    assert 'featureNotSupportedError(string("ALTER ") + phrase)' in source
    assert 'featureNotSupportedError(string("DROP ") + phrase)' in source
    print("[COMPAT OBJECTS] no sidecar writes or fake-success fallback")


if __name__ == "__main__":
    main()
