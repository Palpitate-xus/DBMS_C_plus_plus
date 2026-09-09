#!/usr/bin/env python3
"""Keep every release-version representation internally consistent."""

from pathlib import Path
import re


REPO = Path(__file__).resolve().parents[1]


def required_match(pattern, text, source):
    match = re.search(pattern, text, re.MULTILINE)
    assert match is not None, "%s does not match %s" % (source, pattern)
    return match.group(1)


def main():
    cmake = (REPO / "CMakeLists.txt").read_text(encoding="utf-8")
    header = (REPO / "src/common/version.h").read_text(encoding="utf-8")
    changelog = (REPO / "CHANGELOG.md").read_text(encoding="utf-8")
    package = (REPO / "scripts/package.sh").read_text(encoding="utf-8")

    cmake_version = required_match(
        r"^project\(DBMS_C_plus_plus VERSION ([0-9]+\.[0-9]+\.[0-9]+)",
        cmake, "CMakeLists.txt")
    string_version = required_match(
        r'^#define DBMS_VERSION_STRING "([0-9]+\.[0-9]+\.[0-9]+)"$',
        header, "version.h")
    components = tuple(int(required_match(
        r"^#define DBMS_VERSION_%s ([0-9]+)$" % name,
        header, "version.h")) for name in ("MAJOR", "MINOR", "PATCH"))
    component_version = ".".join(str(value) for value in components)

    assert cmake_version == string_version == component_version
    assert re.search(r"^## \[%s\]" % re.escape(string_version),
                     changelog, re.MULTILINE)
    assert "HEADER_COMPONENT_VERSION" in package
    assert '"$HEADER_COMPONENT_VERSION" != "$HEADER_VERSION"' in package
    print("[VERSION CONSISTENCY] %s" % string_version)


if __name__ == "__main__":
    main()
