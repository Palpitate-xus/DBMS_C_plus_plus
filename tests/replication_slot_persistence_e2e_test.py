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
            [DBMS_MAIN, "--server", "0", "--insecure"],
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

    print("[REPLICATION SLOT PERSISTENCE] corrupt startup state rejected")


if __name__ == "__main__":
    main()
