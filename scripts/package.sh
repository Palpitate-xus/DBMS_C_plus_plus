#!/bin/bash
# Source tarball packaging for DBMS_C_plus_plus.
#
# Guarantees before packing:
#   1. CMakeLists.txt project VERSION  == src/common/version.h
#      DBMS_VERSION_STRING
#   2. CHANGELOG.md carries a matching "## [<version>] - <date>" section
#   3. the worktree is clean (packed content == committed content)
#
# Output: dist/dbms-<version>.tar.gz containing the source tree
# (working files excluded: build artifacts, .git, data directories,
# logs) plus this script's verification stamp.
#
# Usage: scripts/package.sh [output-dir]   (default: dist/)

set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$SRC_DIR/dist}"

# --- 1. version single-source-of-truth checks -----------------------------
CMAKE_VERSION="$(grep -m1 '^project(DBMS' "$SRC_DIR/CMakeLists.txt" \
    | sed -E 's/.*VERSION[[:space:]]+([0-9]+\.[0-9]+\.[0-9]+).*/\1/')"
HEADER_VERSION="$(grep -m1 '#define DBMS_VERSION_STRING' "$SRC_DIR/src/common/version.h" \
    | sed -E 's/.*"([^"]+)".*/\1/')"

if [ -z "$CMAKE_VERSION" ] || [ -z "$HEADER_VERSION" ]; then
    echo "[package] FATAL: could not read version from CMakeLists/version.h" >&2
    exit 1
fi
if [ "$CMAKE_VERSION" != "$HEADER_VERSION" ]; then
    echo "[package] FATAL: version mismatch: CMakeLists=$CMAKE_VERSION version.h=$HEADER_VERSION" >&2
    echo "[package]        fix src/common/version.h and CMakeLists.txt to the same VERSION" >&2
    exit 1
fi

# --- 2. CHANGELOG section present -----------------------------------------
if ! grep -q "^## \[$HEADER_VERSION\]" "$SRC_DIR/CHANGELOG.md"; then
    echo "[package] FATAL: CHANGELOG.md has no '## [$HEADER_VERSION]' section" >&2
    echo "[package]        document the release before packing it" >&2
    exit 1
fi

# --- 3. clean worktree ------------------------------------------------------
if [ -n "$(git -C "$SRC_DIR" status --porcelain 2>/dev/null)" ]; then
    echo "[package] FATAL: worktree not clean; commit or stash first:" >&2
    git -C "$SRC_DIR" status --short >&2
    exit 1
fi

VERSION="$HEADER_VERSION"
mkdir -p "$OUT_DIR"
TARBALL="$OUT_DIR/dbms-$VERSION.tar.gz"

echo "[package] version: $VERSION (CMake == version.h == CHANGELOG ✓, clean tree ✓)"

# --- pack ------------------------------------------------------------------
# Archive from the *committed* tree (git archive) so nothing untracked can
# leak into the tarball.
git -C "$SRC_DIR" archive --format=tar.gz \
    --prefix="dbms-$VERSION/" \
    -o "$TARBALL" HEAD

# --- stamp + verify --------------------------------------------------------
SIZE="$(du -h "$TARBALL" | cut -f1)"
TMP_CHECK="$(mktemp -d)"
tar -xzf "$TARBALL" -C "$TMP_CHECK"
if [ ! -f "$TMP_CHECK/dbms-$VERSION/src/common/version.h" ] || \
   [ ! -f "$TMP_CHECK/dbms-$VERSION/scripts/build.sh" ]; then
    echo "[package] FATAL: tarball content verification failed" >&2
    rm -rf "$TMP_CHECK"
    exit 1
fi
FILES="$(tar -tzf "$TARBALL" | grep -vc '/$')"
rm -rf "$TMP_CHECK"

echo "[package] wrote $TARBALL ($SIZE, $FILES files)"
echo "[package] contents: source tree @ $(git -C "$SRC_DIR" rev-parse --short HEAD)"
echo "[package] build from source: tar xzf $(basename "$TARBALL"); cd dbms-$VERSION; ./scripts/build.sh"
