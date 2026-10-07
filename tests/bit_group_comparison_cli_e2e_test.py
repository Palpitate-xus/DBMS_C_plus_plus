#!/usr/bin/env python3
"""Actual CLI grouped BIT output sorts by bits, with a real empty key and NULL."""
import importlib.util
import os
from pathlib import Path
import re
import tempfile


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location("bit_group_cli",root/"tests/explain_analyze_e2e_test.py")
    cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
    original=os.getcwd()
    try:
        with tempfile.TemporaryDirectory(prefix="dbms-bit-group-cli-") as work:
            os.chdir(work)
            catalog=Path("info/pg_catalog");catalog.mkdir(parents=True)
            (catalog/"pg_authid.cat").write_text(
                '10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'%cli.scram_verifier("admin"),encoding="utf-8")
            Path("info/tlist.lst").touch()
            out,err,rc=cli.run_sql(
                "CREATE DATABASE bit_group_cli;USE DATABASE bit_group_cli;"
                "CREATE TABLE items(v varbit);"
                "INSERT INTO items VALUES (B'1'),(B'01'),(B'001'),(B'0'),(B'00'),(B''),(NULL),(B'01')")
            assert rc==0 and "ERROR" not in out,(rc,out,err)
            ascending=[("","1"),("0","1"),("00","1"),("001","1"),("01","2"),("1","1"),(None,"1")]
            for sql,expected in [
                ("SELECT v,count(*) FROM items GROUP BY v ORDER BY v",ascending),
                ("SELECT v AS bits,count(*) AS n FROM items GROUP BY v ORDER BY bits",ascending),
                ("SELECT v,count(*) FROM items GROUP BY v ORDER BY v DESC",list(reversed(ascending)))]:
                out,err,rc=cli.run_sql("USE DATABASE bit_group_cli;"+sql)
                assert rc==0 and "ERROR" not in out,(sql,rc,out,err)
                rows=[]
                for line in out.splitlines():
                    matched=re.fullmatch(r"\s*([01]+|NULL)?[ \t]+([0-9]+)\s*",line)
                    if matched:
                        rows.append((None if matched[1]=="NULL" else matched[1] or "",matched[2]))
                assert rows==expected,(sql,rows,expected,out)
    finally:
        os.chdir(original)
    print("[BIT GROUP COMPARISON CLI] three complete ordered group/empty/NULL controls passed")


if __name__=="__main__":main()
