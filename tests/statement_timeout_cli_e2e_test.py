#!/usr/bin/env python3
"""The interactive CLI must stop a cooperative long query at its timeout."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "statement_timeout_cli_client", root / "tests/postgres_protocol_test.py")
    client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client)

    with tempfile.TemporaryDirectory(prefix="dbms-statement-timeout-cli-") as work:
        info = Path(work, "info")
        info.mkdir()
        (info / "tlist.lst").touch()
        client.write_auth_catalog(work, "alice", "secret")
        statements = (
            "alice\nsecret\n"
            "SET statement_timeout = 100\n"
            "SELECT 1 FROM generate_series(1, 9223372036854775807) g\n"
            "SELECT 1\n"
            "exit\n"
        )
        binary = Path(os.environ.get("DBMS_MAIN", str(root / "dbms_main"))).resolve()
        result = subprocess.run(
            [str(binary), "--data-dir", work], input=statements,
            text=True, capture_output=True, cwd=work, timeout=15)
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
        assert "ERROR: statement timeout" in result.stdout, result.stdout
        assert "?column? \n1 \n" in result.stdout, result.stdout
        assert result.stdout.find("ERROR: statement timeout") < \
            result.stdout.rfind("?column?"), result.stdout
    print("[STATEMENT TIMEOUT CLI E2E] timeout and recovery passed")


if __name__ == "__main__":
    main()
