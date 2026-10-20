#!/usr/bin/env python3
"""Regression checks for production-object cache routing in build scripts."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile


REPO = Path(__file__).resolve().parents[1]


def check_isolated_data_directory(common):
    """The child must not use a caller-selected data root, even before main."""
    environment_binary = shutil.which("env")
    assert environment_binary
    invocation = r'''
set -u
source "$1"
printf '__ISOLATION_PARENT_BEFORE__=%s\n' "${DBMS_DATA_DIR-__UNSET__}"
printf '__ISOLATION_CWD_BEFORE__=%s\n' "$PWD"
dbms_run_isolated_test data_directory_regression "$2"
task_child_status=$?
printf '__ISOLATION_PARENT_AFTER__=%s\n' "${DBMS_DATA_DIR-__UNSET__}"
printf '__ISOLATION_CWD_AFTER__=%s\n' "$PWD"
exit "$task_child_status"
'''
    checked, failures = 0, []
    with tempfile.TemporaryDirectory(prefix="dbms-helper-data-directory-") as temporary:
        root = Path(temporary).resolve()
        caller = root / "caller"
        caller.mkdir()
        selected_data = caller / "caller data"
        selected_data.mkdir()
        canary = selected_data / "keep.txt"
        canary.write_text("caller-owned canary\n", encoding="utf-8")
        cases = (("absolute", str(selected_data)), ("relative", "caller data"),
                 ("unset", None), ("empty", ""), ("dot", "."))
        for label, selected in cases:
            work = root / ("work " + label)
            work.mkdir()
            environment = dict(PATH=os.environ.get("PATH", os.defpath), TMPDIR=str(work), LC_ALL="C")
            if selected is not None:
                environment["DBMS_DATA_DIR"] = selected
            result = subprocess.run(["bash", "-c", invocation, "isolation-test", str(common), environment_binary],
                                    cwd=caller, env=environment, capture_output=True, text=True)
            fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
            child_cwd = Path(fields["PWD"])
            child_data = fields.get("DBMS_DATA_DIR")
            resolved_data = None if not child_data else Path(child_data)
            if resolved_data is not None and not resolved_data.is_absolute():
                resolved_data = child_cwd / resolved_data
            expected_parent = "__UNSET__" if selected is None else selected
            valid = (result.returncode == 0 and resolved_data is not None
                     and resolved_data.resolve() == child_cwd.resolve()
                     and child_cwd.parent == work and not child_cwd.exists()
                     and fields["__ISOLATION_PARENT_BEFORE__"] == expected_parent
                     and fields["__ISOLATION_PARENT_AFTER__"] == expected_parent
                     and fields["__ISOLATION_CWD_BEFORE__"] == str(caller)
                     and fields["__ISOLATION_CWD_AFTER__"] == str(caller)
                     and canary.read_text(encoding="utf-8") == "caller-owned canary\n")
            checked += 1
            if not valid:
                failures.append(label)
        for expected_status in (0, 7):
            child = root / ("child-" + str(expected_status))
            child.write_text("#!/bin/sh\nexit " + str(expected_status) + "\n", encoding="utf-8")
            child.chmod(0o700)
            work = root / ("status " + str(expected_status))
            work.mkdir()
            environment = dict(PATH=os.environ.get("PATH", os.defpath), TMPDIR=str(work), LC_ALL="C", DBMS_DATA_DIR=str(selected_data))
            result = subprocess.run(["bash", "-c", invocation, "isolation-status", str(common), str(child)],
                                    cwd=caller, env=environment, capture_output=True, text=True)
            checked += 1
            if result.returncode != expected_status or list(work.iterdir()) or canary.read_text(encoding="utf-8") != "caller-owned canary\n":
                failures.append("exit-status-" + str(expected_status))
    assert checked == 7
    print("ISOLATED_TEST_DATA_DIRECTORY_COMPLETE", checked, "FAILURES", len(failures), flush=True)
    assert not failures, failures


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

    # Incremental single-test links must follow the selected source list. A
    # directory glob can pick up both TLSWrapper.o and TLSWrapper_stub.o after
    # a build configuration switch and fail with duplicate definitions.
    assert 'for src in "${DBMS_PROJECT_SOURCES[@]}"' in one
    assert 'link+=("build/obj/$base.o")' in one
    assert "for o in build/obj/*.o" not in one

    for script in ("build_common.sh", "build_one_test.sh", "build_tests.sh"):
        subprocess.run(
            ["bash", "-n", str(REPO / "scripts" / script)], check=True)

    check_isolated_data_directory(REPO / "scripts" / "build_common.sh")

    print("[BUILD CACHE ROUTING] passed")


if __name__ == "__main__":
    main()
