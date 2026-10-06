#!/usr/bin/env python3
"""The CLI must not expose an analyzed plan before statement commit succeeds."""
import base64
import hashlib
import hmac
import os
from pathlib import Path
import subprocess
import tempfile


def verifier(password):
    salt = b"0123456789abcdef"
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 4096)
    client = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    server = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    return "SCRAM-SHA-256$4096:%s$%s:%s" % tuple(base64.b64encode(value).decode()
        for value in (salt, hashlib.sha256(client).digest(), server))


def main():
    root = Path(__file__).resolve().parent.parent
    binary = str(Path(os.environ.get("DBMS_MAIN", root / "dbms_main")).resolve())
    for format_ in ("TEXT", "JSON"):
        with tempfile.TemporaryDirectory(prefix="dbms-explain-publication-cli-") as directory:
            work = Path(directory)
            catalog = work / "info" / "pg_catalog"
            catalog.mkdir(parents=True)
            (catalog / "pg_authid.cat").write_text(
                '10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n' % verifier("admin"), encoding="utf-8")
            (work / "info" / "tlist.lst").touch()
            statements = [
                "CREATE DATABASE explain_publication;", "USE DATABASE explain_publication;",
                "CREATE TABLE parent(id INT PRIMARY KEY);",
                "CREATE TABLE child(id INT PRIMARY KEY,pid INT,CONSTRAINT publication_fk FOREIGN KEY(pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);",
                # The CLI consumes one statement per line. Its existing
                # CREATE FUNCTION parser rejects an optional outer ';';
                # retain the actual body delimiters/terminators, not that
                # separate command-reader boundary in this publication test.
                "CREATE FUNCTION explain_cli_deferred(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO child VALUES(arg,999); RETURN arg; END; $$",
                f"EXPLAIN (ANALYZE TRUE,FORMAT {format_}) SELECT explain_cli_deferred(1);",
                "SELECT id FROM child;", "SELECT 42;", "exit",
            ]
            env = dict(os.environ, DBMS_COMPATIBILITY_MODE="extended")
            result = subprocess.run([binary, "--data-dir", directory],
                input="admin admin\n" + "\n".join(statements) + "\n", cwd=directory,
                text=True, capture_output=True, timeout=60, env=env)
            assert "23503" in result.stdout, (format_, result.stdout, result.stderr)
            assert "statement transaction commit failed" in result.stdout, (format_, result.stdout)
            assert "--- ANALYZE ---" not in result.stdout and "Actual rows:" not in result.stdout, (format_, result.stdout)
            assert '"nodeType"' not in result.stdout, (format_, result.stdout)
            # The failed statement left the child empty; the following scalar
            # query still succeeds. Keep this separate from no-plan framing.
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            assert lines[-3:] == ["id", "?column?", "42"], (format_, result.stdout)
            assert "CREATE FUNCTION succeeded" in result.stdout, (format_, result.stdout)
            assert result.returncode == 0, (format_, result.returncode, result.stdout, result.stderr)
            print("[EXPLAIN PUBLICATION CLI]", format_, "passed", flush=True)


if __name__ == "__main__":
    main()
