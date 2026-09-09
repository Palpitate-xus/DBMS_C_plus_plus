#!/usr/bin/env python3
"""Every standalone E2E runner honors the selected production binary."""

import importlib.util
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUNNERS = (
    "explain_analyze_e2e_test.py",
    "inherit_only_e2e_test.py",
    "multijoin_e2e_test.py",
    "postgres_protocol_test.py",
    "timestamptz_e2e_test.py",
    "unnest_e2e_test.py",
    "window_e2e_test.py",
)


def main():
    sentinel = str(ROOT / "build" / "selected-e2e-binary")
    previous = os.environ.get("DBMS_MAIN")
    os.environ["DBMS_MAIN"] = sentinel
    try:
        for index, filename in enumerate(RUNNERS):
            path = ROOT / "tests" / filename
            spec = importlib.util.spec_from_file_location(
                "e2e_binary_route_%d" % index, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            assert module.DBMS_MAIN == os.path.abspath(sentinel), (
                filename, module.DBMS_MAIN, sentinel)
    finally:
        if previous is None:
            os.environ.pop("DBMS_MAIN", None)
        else:
            os.environ["DBMS_MAIN"] = previous
    print("[E2E BINARY ROUTING] passed")


if __name__ == "__main__":
    main()
