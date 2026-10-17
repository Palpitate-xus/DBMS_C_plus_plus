"""Real CLI cold reads preserve microseconds, timezone and spaced siblings."""
import importlib.util
import os
from pathlib import Path
import tempfile


def main():
    path = Path(__file__).with_name("timestamptz_e2e_test.py")
    spec = importlib.util.spec_from_file_location("temporal_cli_support", path)
    support = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(support)
    previous = os.getcwd()
    checks, failures = 0, []
    try:
        with tempfile.TemporaryDirectory(prefix="dbms-micro-cli-") as directory:
            os.chdir(directory)
            os.makedirs("info/pg_catalog")
            with open("info/pg_catalog/pg_authid.cat", "w", encoding="utf-8") as output:
                output.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n' % support.scram_verifier("admin"))
            Path("info/tlist.lst").touch()
            def check(good, label, detail):
                nonlocal checks
                checks += 1
                print("TEMPORAL_HEAP_CLI", checks, label, "match", good, repr(detail), flush=True)
                if not good:
                    failures.append((label, detail))
            out, err, status = support.run_sql([
                "CREATE DATABASE micros", "USE DATABASE micros",
                "CREATE TABLE events(id integer,p timestamp,t time,z timestamptz,note text)",
                "INSERT INTO events VALUES(1,'1999-12-31 23:59:59.999999','24:00:00',"
                "'2000-01-01 00:00:00.000001+00','two words')"])
            check(status == 0 and "ERROR" not in out and "ERROR" not in err,
                  "new SQL heap setup", (status, out, err))
            for zone, value in (("UTC", "2000-01-01 00:00:00.000001+00"),
                                ("Asia/Kathmandu", "2000-01-01 05:45:00.000001+05:45"),
                                ("America/New_York", "1999-12-31 19:00:00.000001-05")):
                out, err, status = support.run_sql(["USE DATABASE micros",
                    "SET TIME ZONE '" + zone + "'", "SELECT id,p,t,z,note FROM events ORDER BY id"])
                expected = "1 1999-12-31 23:59:59.999999 24:00:00 " + value + " two words"
                rows = [line.strip().replace(chr(34), "") for line in out.splitlines()]
                check(status == 0 and "ERROR" not in out and "ERROR" not in err and expected in rows,
                      "fresh process " + zone + " exact typed cells", (status, expected, out, err))
            print("TEMPORAL_HEAP_CLI_COMPLETE", checks, "FAILURES", len(failures), flush=True)
            assert checks == 4 and not failures, failures
    finally:
        os.chdir(previous)


if __name__ == "__main__":
    main()
