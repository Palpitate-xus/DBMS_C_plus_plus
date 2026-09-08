#!/usr/bin/env python3
"""P0-16 differential compatibility runner.

Drives the same SQL case files against a reference PostgreSQL (docker
`pgref` container, psql) and this DBMS (wire protocol), comparing decoded
rows without changing values, SQLSTATE, and optionally column headers.

Usage:
    python3 tests/compat/pg_diff_runner.py [--case-dir DIR] [--only NAME]

Case files (tests/compat/cases/*.sql) contain one statement per line;
lines starting with `--` are comments. The reference currently reconnects
for every statement; session/transaction cases and lossless reference row
decoding remain work in progress (P0-16). Differences are reported directly;
this runner does not implement an allowlist or command-tag comparison yet.
"""

import argparse
import csv
import importlib.util
import io
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
    """Run one statement on reference PG; return (rows, sqlstate, tag, diagnostic)."""
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER,
         "psql", "-U", "postgres", "-d", "postgres",
         "-v", "ON_ERROR_STOP=0", "-v", "VERBOSITY=sqlstate", "-X", "-q", "-A", "-t",
         "-F", "\x1f", "-P", "null=NULLMARK"],
        input=sql.encode(), capture_output=True)
    out = proc.stdout.decode()
    err = proc.stderr.decode()
    # Only the final line terminator is framing. Earlier empty lines are
    # empty text rows, while SQL NULL currently uses the separate marker.
    segs = out.split("\n")
    if segs and segs[-1] == "":
        segs = segs[:-1]
    # -q suppresses command tags; any command-looking line here is data.
    # Filtering by a tag regex discarded values such as "CREATE TABLE".
    rows = []
    for line in segs:
        vals = line.split("\x1f")
        rows.append([None if v == "NULLMARK" else v for v in vals])
    state = None
    # psql's sqlstate verbosity prints "ERROR:  22012". Default verbosity
    # has no code, so previously every two errors were treated as equivalent.
    # Do not infer SQLSTATE from translated message text or NOTICE/WARNING.
    m = re.search(r"^ERROR:\s+([0-9A-Z]{5})\s*$", err, re.M)
    if m:
        state = m.group(1)
    elif any(ln.startswith("ERROR:") for ln in err.splitlines()):
        # Unknown is a mismatch, not a wildcard for any error from our server.
        state = "ERROR"
    if proc.returncode != 0 and state is None:
        raise RuntimeError("reference psql failed: " + err.strip())
    tag = None
    return rows, state, tag, err.strip()


def reference_headers(sql):
    """Describe one statement without executing volatile or modifying SQL twice."""
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER,
         "psql", "-U", "postgres", "-d", "postgres",
         "-v", "ON_ERROR_STOP=0", "-X", "-q", "--csv"],
        input=(sql.rstrip().rstrip(";") + "\n\\gdesc\n").encode(), capture_output=True)
    out = proc.stdout.decode()
    err = proc.stderr.decode()
    if proc.returncode != 0 or re.search(r"^(ERROR|FATAL):", err, re.M):
        raise RuntimeError("reference describe failed: " + err.strip())
    if out.strip() == "The command has no result, or the result has no columns.":
        return []
    # The descriptor's Column/Type table is CSV, not query-result rows. Names
    # can themselves contain commas, quotes or newlines without ambiguity.
    rows = list(csv.reader(io.StringIO(out, newline=""), strict=True))
    if not rows or rows[0] != ["Column", "Type"] or any(len(row) != 2 for row in rows[1:]):
        raise RuntimeError("unexpected reference descriptor output: " + repr(out))
    return [row[0] for row in rows[1:]]


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
    headers = []
    for kind, body in messages:
        if kind == b"T":
            # RowDescription: per field: name NUL, then table oid (i16),
            # attr number (i16), type oid (i32), typlen (i16), typmod (i32),
            # format code (i16).
            n = int.from_bytes(body[0:2], "big")
            off = 2
            for _ in range(n):
                z = body.index(b"\x00", off)
                headers.append(body[off:z].decode())
                off = z + 1 + 18
        elif kind == b"D":
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
    return rows, state, message, headers


# --------------------------------------------------------------- normalizing

def normalize_rows(rows):
    # DataRow already distinguishes NULL (-1) from a zero-byte value. The
    # reference reader also has a separate NULL marker. Changing values here
    # hid real result mismatches, including empty text vs NULL and ordinary
    # user strings such as "OID 123". Normalize container shape only.
    return [list(row) for row in rows]


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
    compare_headers = os.environ.get("PGDIFF_HEADERS", "1") == "1"

    ours = []
    for sql in stmts:
        rows, state, message, ohead = ours_query(client, sock, sql)
        ours.append((rows, state, message, ohead))

    for sql, (rrows, rstate, rtag, rerr), (orows, ostate, omsg, ohead) in zip(stmts, ref, ours):
        if normalize_rows(rrows) != normalize_rows(orows):
            diffs.append("%s: rows differ\n  PG:   %r\n  ours: %r" % (sql, rrows, orows))
        if rstate != ostate:
            diffs.append("%s: sqlstate differs: PG=%r ours=%r" % (sql, rstate, ostate))
        if compare_headers and rstate is None and ostate is None and orows and ohead:
            rhead = reference_headers(sql)
            if rhead and rhead != ohead:
                diffs.append("%s: headers differ\n  PG:   %r\n  ours: %r" % (sql, rhead, ohead))
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
