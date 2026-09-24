#!/usr/bin/env python3
"""Exercise content-addressed production objects in an isolated tiny build."""

from pathlib import Path
import subprocess
import tempfile


REPO = Path(__file__).resolve().parents[1]
BUILD_COMMON = REPO / "scripts" / "build_common.sh"


def run_build(root: Path, link_flag: str = "") -> str:
    script = r'''
set -euo pipefail
. "$2"
DBMS_SOURCE_DIR="$1"
DBMS_MANIFEST="$1/cmake/dbms_sources.txt"
DBMS_MAIN_SOURCES=(src/main.cpp src/answer.cpp)
DBMS_MANIFEST_SOURCES=(src/main.cpp)
DBMS_TLS_SOURCE=src/answer.cpp
DBMS_CXXFLAGS=(-std=c++17 -O0)
DBMS_PRODUCTION_INCLUDES=(-Isrc)
DBMS_TEST_INCLUDES=(-Isrc)
DBMS_LDFLAGS=()
if [[ -n "$3" ]]; then DBMS_LDFLAGS+=("$3"); fi
DBMS_HAS_ZLIB=0
dbms_build_main
'''
    result = subprocess.run(
        ["bash", "-c", script, "build-test", str(root),
         str(BUILD_COMMON), link_flag],
        check=True, capture_output=True, text=True,
    )
    return result.stdout


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="dbms-main-build-test-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        (root / "cmake").mkdir()
        (root / "cmake/dbms_sources.txt").write_text(
            "src/main.cpp\n", encoding="utf-8")
        header = root / "src/answer.h"
        header.write_text("int answer();\n", encoding="utf-8")
        main_source = root / "src/main.cpp"
        main_source.write_text(
            '#include "answer.h"\n#include <iostream>\n'
            'int main() { std::cout << answer() << "\\n"; }\n',
            encoding="utf-8",
        )
        (root / "src/answer.cpp").write_text(
            '#include "answer.h"\nint answer() { return 1; }\n',
            encoding="utf-8",
        )

        first = run_build(root)
        assert "Compiling src/main.cpp" in first
        assert "Compiling src/answer.cpp" in first
        assert subprocess.check_output([root / "dbms_main"], text=True) == "1\n"

        main_stamp = root / "build/main_obj/src/main.cpp.o.sha256"
        answer_stamp = root / "build/main_obj/src/answer.cpp.o.sha256"
        main_before = main_stamp.read_text(encoding="utf-8")
        answer_before = answer_stamp.read_text(encoding="utf-8")
        assert "up to date" in run_build(root)

        main_source.write_text(
            '#include "answer.h"\n#include <iostream>\n'
            'int main() { std::cout << answer() + 1 << "\\n"; }\n',
            encoding="utf-8",
        )
        changed_source = run_build(root)
        assert "Compiling src/main.cpp" in changed_source
        assert "Compiling src/answer.cpp" not in changed_source
        assert main_stamp.read_text(encoding="utf-8") != main_before
        assert answer_stamp.read_text(encoding="utf-8") == answer_before
        assert subprocess.check_output([root / "dbms_main"], text=True) == "2\n"

        header.write_text("int answer();\n// changed\n", encoding="utf-8")
        changed_header = run_build(root)
        assert "Compiling src/main.cpp" in changed_header
        assert "Compiling src/answer.cpp" in changed_header
        assert answer_stamp.read_text(encoding="utf-8") != answer_before

        changed_link = run_build(root, "-pthread")
        assert "Compiling src/" not in changed_link
        assert "Linking production binary" in changed_link

    print("[MAIN BUILD INCREMENTAL] passed")


if __name__ == "__main__":
    main()
