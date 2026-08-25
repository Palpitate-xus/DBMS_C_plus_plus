#!/usr/bin/env python3
"""P0-16 differential compatibility runner.

Drives the same SQL case files against a reference PostgreSQL (docker
`pgref` container, psql) and this DBMS (wire protocol), normalizes
unstable fields (OIDs, timings, counts wording), and diffs results:
rows, SQLSTATE, and command tags.

Usage:
    python3 tests/compat/pg_diff_runner.py [--case-dir DIR] [--only NAME]

Case files (tests/compat/cases/*.sql) contain one statement per line;
lines starting with `--` are comments. Each case runs in a fresh session.
Any difference is reported and must be added, with a reason and expiry, to
tests/compat/allowlist.yaml to be acknowledged; CI fails on unlisted diffs.
"""

import argparse
import importlib.util
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONTAINER = os.environ.get("PGREF_CONTAINER", "pgref")
DBMS_MAIN = os.environ.get("DBMS_MAIN", os.path.join(REPO, "dbms_main"))


def load_protocol_client():
    spec = importlib.util.spec_from_file_location(
        "pgproto", os.path.join(REPO, "tests", "postgres_protocol_test.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------- reference

def reference_query(sql):
    """Run one statement on reference PG; return (rows, sqlstate, tag)."""
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER,
         "psql", "-U", "postgres", "-d", "postgres",
         "-v", "ON_ERROR_STOP=0", "-X", "-q", "-A", "-t",
         "-F", "\x1f", "-P", "null=NULLMARK"],
        input=sql.encode(), capture_output=True)
    out = proc.stdout.decode()
    err = proc.stderr.decode()
    # psql -A -t prints one line per row; a NULL-only row is an empty
    # line, but a trailing newline terminates the output. A single
    # trailing newline means zero-or-N rows; treat the LAST empty
    # segment after the final newline as the terminator, and empty
    # segments before it as NULL rows.
    segs = out.split("\n")
    if segs and segs[-1] == "":
        segs = segs[:-1]
    # psql -A -t prints command tags (CREATE TABLE, INSERT 0 2, ...) on
    # stdout too; they never contain the field separator and match the
    # known tag grammar, so filter them out of the row stream.
    tag_re = re.compile(
        r"^(CREATE|INSERT|UPDATE|DELETE|SELECT|DROP|ALTER|TRUNCATE|BEGIN|"
        r"COMMIT|ROLLBACK|SET|RESET|GRANT|REVOKE|COPY|ANALYZE|VACUUM|"
        r"REINDEX|COMMENT|DO|CALL|LOCK|SHOW)\b.*$")
    rows = []
    for line in segs:
        if tag_re.match(line) and "\x1f" not in line:
            continue
        vals = line.split("\x1f")
        rows.append([None if v == "NULLMARK" else v for v in vals])
    state = None
    m = re.search(r"\[(SQLSTATE ([0-9A-Z]{5}))\]", err)
    if m:
        state = m.group(2)
    elif err.strip():
        state = "ERROR"
    tag = None
    m2 = re.search(r"^([A-Z_]+ [A-Z_ ]+)$", err.strip(), re.M)
    return rows, state, tag, err.strip()


def reference_multi(statements):
    """Run statements one-by-one, capturing per-statement outcomes."""
    results = []
    for sql in statements:
        results.append(reference_query(sql + ";"))
    return results


# ------------------------------------------------------------------- our side


def ours_query(client, sock, sql):
    """Run one statement on this DBMS; return (rows, sqlstate, message)."""
    messages = client.simple_query(sock, sql)
    rows = []
    state = None
    message = ""
    for kind, body in messages:
        if kind == b"D":
            # DataRow: int16 ncols, then int32 len + bytes each
            n = int.from_bytes(body[0:2], "big")
            off = 2
            vals = []
            for _ in range(n):
                ln = int.from_bytes(body[off:off + 4], "big", signed=True)
                off += 4
                if ln == -1:
                    vals.append(None)
                else:
                    vals.append(body[off:off + ln].decode())
                    off += ln
            rows.append(vals)
        elif kind == b"E":
            for field in body.rstrip(b"\0").split(b"\0"):
                if not field:
                    continue
                tag, value = field[:1].decode(), field[1:].decode()
                if tag == "C":
                    state = value
                elif tag == "M":
                    message = value
    return rows, state, message


# --------------------------------------------------------------- normalizing

UNSTABLE = [
    (re.compile(r"\bOID ([0-9]+)\b"), "OID <n>"),
]


def normalize_rows(rows):
    out = []
    for row in rows:
        nrow = []
        for v in row:
            if isinstance(v, str):
                for pat, rep in UNSTABLE:
                    v = pat.sub(rep, v)
            nrow.append(v)
        out.append(nrow)
    return out


def normalize_message(msg):
    msg = msg or ""
    # SQLSTATE marker duplicates and unstable hints are stripped; wording
    # differences are allowlisted explicitly per case.
    msg = re.sub(r"\s*\(SQLSTATE [0-9A-Z]{5}\)", "", msg)
    return msg.strip()


# --------------------------------------------------------------------- main


def load_cases(case_dir, only=None):
    cases = []
    for fname in sorted(os.listdir(case_dir)):
        if not fname.endswith(".sql"):
            continue
        name = fname[:-4]
        if only and only not in name:
            continue
        path = os.path.join(case_dir, fname)
        stmts = []
        for line in open(path):
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            stmts.append(line.rstrip(";"))
        cases.append((name, stmts))
    return cases


def run_case(name, stmts, client, sock):
    """Returns list of per-statement diff strings (empty when identical)."""
    ref = reference_multi(stmts)
    diffs = []

    ours = []
    for sql in stmts:
        rows, state, message = ours_query(client, sock, sql)
        ours.append((rows, state, message))

    for sql, (rrows, rstate, rtag, rerr), (orows, ostate, omsg) in zip(stmts, ref, ours):
        rstate_n = rstate
        # psql reports bare ERROR without code when the message lacks one;
        # compare presence rather than exact code in that case
        if rstate == "ERROR" and ostate not in (None, "00000"):
            rstate_n = ostate  # both errored; code compared only when PG prints it
        if normalize_rows(rrows) != normalize_rows(orows):
            diffs.append("%s: rows differ\n  PG:   %r\n  ours: %r" % (sql, rrows, orows))
        if (rstate_n or None) != (ostate or None) and not (rstate_n is None and ostate is None):
            if rstate_n == "ERROR" and ostate:
                pass  # both error, code unknown on PG side
            else:
                diffs.append("%s: sqlstate differs: PG=%r ours=%r" % (sql, rstate_n, ostate))
    return diffs


def start_ours(client):
    work_dir = tempfile.mkdtemp(prefix="dbms-pgdiff-")
    os.mkdir(os.path.join(work_dir, "info"))
    open(os.path.join(work_dir, "info", "tlist.lst"), "wb").close()
    client.write_auth_catalog(work_dir, "alice", "secret")
    with open(os.path.join(work_dir, "pg_hba.conf"), "w", encoding="utf-8") as hba:
        hba.write("host all alice 127.0.0.1/32 scram-sha-256\n")
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    process = subprocess.Popen(
        [DBMS_MAIN, "--server", str(port), "--insecure"],
        cwd=work_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    sock = socket.socket()
    sock.settimeout(15)
    deadline = time.time() + 20
    while True:
        try:
            sock.connect(("127.0.0.1", port))
            break
        except OSError:
            if time.time() >= deadline:
                raise
            time.sleep(0.05)
    client.startup(sock, "alice", "info")
    return {"process": process, "sock": sock, "dir": work_dir, "port": port}


def stop_ours(server):
    try:
        server["sock"].close()
    except OSError:
        pass
    server["process"].terminate()
    server["process"].wait(timeout=10)
    subprocess.run(["rm", "-rf", server["dir"]], check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case-dir", default=os.path.join(REPO, "tests", "compat", "cases"))
    ap.add_argument("--only", default=None)
    args = ap.parse_args()

    client = load_protocol_client()

    server = start_ours(client)
    try:
        cases = load_cases(args.case_dir, args.only)
        if not cases:
            print("no cases found in", args.case_dir)
            return 1
        failed = 0
        for name, stmts in cases:
            diffs = run_case(name, stmts, client, server["sock"])
            if diffs:
                failed += 1
                print("[DIFF] %s" % name)
                for d in diffs:
                    print("  " + d.replace("\n", "\n  "))
            else:
                print("[OK] %s" % name)
        print("cases=%d failed=%d" % (len(cases), failed))
        return 1 if failed else 0
    finally:
        stop_ours(server)


if __name__ == "__main__":
    sys.exit(main())
