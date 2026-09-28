#!/usr/bin/env python3
"""An ordinary SQL error must not terminate the interactive DBMS process."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "cli_pg_client", root / "tests" / "postgres_protocol_test.py")
    client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client)

    with tempfile.TemporaryDirectory(prefix="dbms-cli-error-") as work:
        info = Path(work, "info")
        info.mkdir()
        (info / "tlist.lst").touch()
        client.write_auth_catalog(work, "alice", "secret")
        statements = (
            "alice\nsecret\n"
            "CREATE TABLE cli_base (id INT);\n"
            "INSERT INTO cli_base VALUES (1);\n"
            "CREATE MATERIALIZED VIEW cli_mv AS "
            "SELECT id FROM cli_base WITH NO DATA;\n"
            "SELECT * FROM cli_mv;\n"
            "SELECT 1;\n"
            "exit\n"
        )
        binary = Path(os.environ.get("DBMS_MAIN", str(root / "dbms_main"))).resolve()
        result = subprocess.run(
            [str(binary), "--data-dir", work], input=statements,
            text=True, capture_output=True, cwd=work, timeout=60)
        diagnostic = (
            'ERROR: materialized view "public.cli_mv" has not been populated '
            '(SQLSTATE 55000)'
        )
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
        assert diagnostic in result.stdout, result.stdout
        assert result.stdout.find(diagnostic) < result.stdout.find("?column?"), result.stdout
        assert "?column? \n1 \n" in result.stdout, result.stdout
    print("[CLI ERROR RECOVERY E2E] passed")


if __name__ == "__main__":
    main()
