#!/usr/bin/env python3
"""Regression checks for production-object cache routing in build scripts."""

from pathlib import Path
import subprocess


REPO = Path(__file__).resolve().parents[1]


def main():
    one = (REPO / "scripts" / "build_one_test.sh").read_text(encoding="utf-8")
    all_tests = (REPO / "scripts" / "build_tests.sh").read_text(encoding="utf-8")
    common = (REPO / "scripts" / "build_common.sh").read_text(encoding="utf-8")

    for name, script in (("build_one_test.sh", one),
                         ("build_tests.sh", all_tests)):
        assert "dbms_cache_needs_rebuild" in script, name
        assert "dbms_write_cache_signature" in script, name
        assert "dbms_test_cache_needs_rebuild" not in script, name
        assert "dbms_write_test_cache_signature" not in script, name

    # A test source is always compiled by each driver; it does not belong in
    # the signature for the reusable production object layer.
    assert "dbms_test_cache_signature" not in common
    assert one.index("dbms_write_cache_signature") < one.index(
        "# 编译测试本身")
    assert all_tests.index("dbms_write_cache_signature") < all_tests.index(
        "for test_file in tests/*_test.cpp")

    for script in ("build_common.sh", "build_one_test.sh", "build_tests.sh"):
        subprocess.run(
            ["bash", "-n", str(REPO / "scripts" / script)], check=True)

    print("[BUILD CACHE ROUTING] passed")


if __name__ == "__main__":
    main()
