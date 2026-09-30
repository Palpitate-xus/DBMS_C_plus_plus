#!/usr/bin/env python3
"""Basic CLI DISTINCT uses the same typed identity before OFFSET/LIMIT."""

import importlib.util
import os
from pathlib import Path
import re
import tempfile


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_distinct_cli", root / "tests" / "explain_analyze_e2e_test.py")
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    original = os.getcwd()
    try:
        with tempfile.TemporaryDirectory(prefix="dbms-typed-distinct-cli-") as work:
            os.chdir(work)
            catalog = Path("info/pg_catalog")
            catalog.mkdir(parents=True)
            (catalog / "pg_authid.cat").write_text(
                '10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n' % cli.scram_verifier("admin"),
                encoding="utf-8")
            Path("info/tlist.lst").touch()
            out, err, rc = cli.run_sql(
                "CREATE DATABASE typed_distinct_cli; USE DATABASE typed_distinct_cli;"
                "CREATE TABLE items(v NUMERIC);"
                "INSERT INTO items VALUES (0.50),(0.500),(2.00),(NULL)")
            assert rc == 0 and "ERROR" not in out, (rc, out, err)
            for sql, expected in [
                ("SELECT DISTINCT v FROM items ORDER BY v", ["0.50", "2.00"]),
                ("SELECT DISTINCT v FROM items ORDER BY v LIMIT 1 OFFSET 1", ["2.00"]),
            ]:
                out, err, rc = cli.run_sql("USE DATABASE typed_distinct_cli; " + sql)
                assert rc == 0 and "ERROR" not in out, (sql, rc, out, err)
                values = [line.strip() for line in out.splitlines()
                          if re.fullmatch(r"\s*-?[0-9]+(?:\.[0-9]+)?\s*", line)]
                assert values == expected, (sql, values, expected, out)
    finally:
        os.chdir(original)
    print("[TYPED SELECT DISTINCT CLI E2E] passed")


if __name__ == "__main__":
    main()
