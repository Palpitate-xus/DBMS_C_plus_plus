#!/usr/bin/env python3
"""Server startup must reject a corrupt durable replication-slot catalog."""

import os
from pathlib import Path
import subprocess
import tempfile


DBMS_MAIN = os.path.abspath(os.environ.get(
    "DBMS_MAIN", os.path.join(os.path.dirname(__file__), "..", "dbms_main")))


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")

    with tempfile.TemporaryDirectory(prefix="dbms-slot-state-") as work_dir:
        Path(work_dir, ".replication_slots").write_text(
            "BROKEN_SLOT_STATE\n", encoding="utf-8")
        result = subprocess.run(
            [DBMS_MAIN, "--data-dir", work_dir,
             "--server", "0", "--insecure"],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 1, result
        assert "FATAL: invalid replication slot state header" in result.stderr, \
            result.stderr

        Path(work_dir, ".replication_slots").write_text(
            "DBMS_REPLICATION_SLOTS_V1\n"
            '"bad_plugin" "logical" "missing_plugin" "db" 0\n',
            encoding="utf-8")
        result = subprocess.run(
            [DBMS_MAIN, "--data-dir", work_dir,
             "--server", "0", "--insecure"],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 1, result
        assert "FATAL: invalid replication slot state entry" in result.stderr, \
            result.stderr

        Path(work_dir, ".replication_slots").write_text(
            "DBMS_REPLICATION_SLOTS_V2\n"
            '"physical_invalidated" "physical" "" "" 0 1\n',
            encoding="utf-8")
        result = subprocess.run(
            [DBMS_MAIN, "--data-dir", work_dir,
             "--server", "0", "--insecure"],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 1, result
        assert "FATAL: invalid replication slot state entry" in result.stderr, \
            result.stderr

        Path(work_dir, ".replication_slots").write_text(
            "DBMS_REPLICATION_SLOTS_V2\n"
            '"bad_flag" "logical" "dbms_test_decoding" "db" 0 2\n',
            encoding="utf-8")
        result = subprocess.run(
            [DBMS_MAIN, "--data-dir", work_dir,
             "--server", "0", "--insecure"],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 1, result
        assert "FATAL: invalid replication slot state entry" in result.stderr, \
            result.stderr

    print("[REPLICATION SLOT PERSISTENCE] corrupt startup state rejected")


if __name__ == "__main__":
    main()
