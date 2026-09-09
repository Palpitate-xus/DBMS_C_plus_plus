#!/usr/bin/env python3
"""Prevent compatibility-record persistence and fake-success paths returning."""

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def main():
    source = (REPO / "src/main.cpp").read_text(encoding="utf-8")
    header = (REPO / "src/common/FeatureGate.h").read_text(encoding="utf-8")

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
    assert "A runtime-backed CREATE must be consumed" in source
    assert 'featureNotSupportedError(string("CREATE ") + phrase)' in source
    assert 'featureNotSupportedError(string("ALTER ") + phrase)' in source
    assert 'featureNotSupportedError(string("DROP ") + phrase)' in source
    print("[COMPAT OBJECTS] no sidecar writes or fake-success fallback")


if __name__ == "__main__":
    main()
