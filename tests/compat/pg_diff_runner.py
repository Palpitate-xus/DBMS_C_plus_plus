#!/usr/bin/env python3
"""P0-16 differential compatibility runner.

Drives the same SQL case files against a reference PostgreSQL (`pgref`
container) and this DBMS through their wire protocols, comparing decoded
rows without changing values, SQLSTATE, and optionally column headers.

Usage:
    python3 tests/compat/pg_diff_runner.py [--case-dir DIR] [--only NAME]

Case files (tests/compat/cases/*.sql) contain one statement per line;
lines starting with `--` are comments. The reference uses one connection
per case so session/transaction state is preserved. Lossless reference
row values, NULL metadata and command tags without delimiter parsing.
Differences are reported directly; this runner does not implement an allowlist.
"""

import argparse
import csv
import importlib.util
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import uuid

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


def describe_statement(sql):
    """Remove the sole SQL terminator, never letting psql execute it before gdesc.

    Quotes/comments matter here: rstrip(';') leaves a terminator followed by
    a comment, while splitting on every semicolon corrupts literal values.
    Reference sessions use PostgreSQL's default standard-conforming strings;
    E-strings, dollar quotes and nested block comments are handled explicitly.
    """
    end = None
    i = 0
    identifier_char = lambda c: c.isalnum() or c in "_$"
    while i < len(sql):
        if sql[i].isspace():
            i += 1
            continue
        if sql.startswith("--", i):
            while i < len(sql) and sql[i] not in "\r\n":
                i += 1
            continue
        if sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(sql) and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise RuntimeError("unterminated comment in reference descriptor query")
            continue
        if sql[i] == ";":
            if end is None:
                end = i
            i += 1
            continue
        if end is not None:
            raise RuntimeError("reference description requires exactly one SQL statement")
        if sql[i] in "'\"":
            quote = sql[i]
            escaped = quote == "'" and i > 0 and sql[i - 1] in "eE" and (
                i < 2 or not identifier_char(sql[i - 2]))
            i += 1
            while i < len(sql):
                if escaped and sql[i] == "\\":
                    i += 2
                elif sql[i] == quote:
                    i += 1
                    if i < len(sql) and sql[i] == quote:
                        i += 1
                    else:
                        break
                else:
                    i += 1
            else:
                raise RuntimeError("unterminated quote in reference descriptor query")
            continue
        if sql[i] == "$" and (i == 0 or not identifier_char(sql[i - 1])):
            tag = re.match(r"\$(?:[^\W\d]\w*)?\$", sql[i:])
            if tag:
                delimiter = tag.group(0)
                close = sql.find(delimiter, i + len(delimiter))
                if close < 0:
                    raise RuntimeError("unterminated dollar quote in reference descriptor query")
                i = close + len(delimiter)
                continue
        if sql[i] == "\\":
            raise RuntimeError("psql meta-commands are not reference descriptor SQL")
        i += 1
    statement = sql[:end].rstrip() if end is not None else sql.rstrip()
    if not statement.strip():
        raise RuntimeError("reference description requires a SQL statement")
    return statement


def reference_headers(sql):
    """Describe one statement without executing volatile or modifying SQL twice."""
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER,
         "psql", "-U", "postgres", "-d", "postgres",
         "-v", "ON_ERROR_STOP=0", "-X", "-q", "--csv"],
        input=(describe_statement(sql) + "\n\\gdesc\n").encode(), capture_output=True)
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


def _reference_psql_multi(statements):
    """Legacy psql framing retained only for focused parser unit tests."""
    token = uuid.uuid4().hex
    script = []
    begin_markers = []
    descriptor_end_markers = []
    error_begin_markers = []
    error_end_markers = []
    end_prefixes = []
    for index, sql in enumerate(statements):
        statement = describe_statement(sql)
        begin = "__PGDIFF_%s_BEGIN_%d__" % (token, index)
        end = "__PGDIFF_%s_END_%d__" % (token, index)
        descriptor_end = "__PGDIFF_%s_DESC_END_%d__" % (token, index)
        error_begin = "__PGDIFF_%s_ERROR_BEGIN_%d__" % (token, index)
        error_end = "__PGDIFF_%s_ERROR_END_%d__" % (token, index)
        begin_markers.append(begin)
        descriptor_end_markers.append(descriptor_end)
        error_begin_markers.append(error_begin)
        error_end_markers.append(error_end)
        end_prefixes.append(end)
        script.extend([
            "\\echo " + begin,
            statement,
            "\\gdesc",
            "\\echo " + descriptor_end,
            "\\warn " + error_begin,
            "\\g",
            "\\warn " + error_end,
            "\\echo " + end + " :ERROR :SQLSTATE :ROW_COUNT",
        ])

    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER,
         "psql", "-U", "postgres", "-d", "postgres",
         "-v", "ON_ERROR_STOP=0", "-v", "VERBOSITY=sqlstate",
         "-X", "-q", "-A", "-t", "-F", "\x1f",
         "-P", "null=NULLMARK"],
        input=("\n".join(script) + "\n").encode(), capture_output=True)
    stderr = proc.stderr.decode()
    if proc.returncode != 0:
        raise RuntimeError("reference psql failed: " + stderr.strip())

    lines = proc.stdout.decode().splitlines()
    results = []
    cursor = 0
    stderr_lines = stderr.splitlines()
    stderr_cursor = 0
    for index, (begin, descriptor_end, error_begin, error_end, end) in enumerate(zip(
            begin_markers, descriptor_end_markers, error_begin_markers,
            error_end_markers, end_prefixes)):
        try:
            begin_at = lines.index(begin, cursor)
        except ValueError as exc:
            raise RuntimeError("missing reference result marker: " + begin) from exc
        try:
            descriptor_end_at = lines.index(descriptor_end, begin_at + 1)
        except ValueError as exc:
            raise RuntimeError(
                "missing reference descriptor marker: " + descriptor_end) from exc
        end_at = None
        status_match = None
        for position in range(descriptor_end_at + 1, len(lines)):
            if not lines[position].startswith(end + " "):
                continue
            status_match = re.fullmatch(
                re.escape(end) + r"\s+(true|false)\s+([0-9A-Z]{5})\s+(\d+)",
                lines[position])
            if status_match:
                end_at = position
                break
        if end_at is None or status_match is None:
            raise RuntimeError("missing reference status marker: " + end)

        descriptor_lines = lines[begin_at + 1:descriptor_end_at]
        no_result = "The command has no result, or the result has no columns."
        headers = [] if descriptor_lines == [no_result] else [
            line.split("\x1f", 1)[0] for line in descriptor_lines
        ]
        rows = []
        for line in lines[descriptor_end_at + 1:end_at]:
            rows.append([None if value == "NULLMARK" else value
                         for value in line.split("\x1f")])
        failed = status_match.group(1) == "true"
        state = status_match.group(2) if failed else None
        try:
            error_begin_at = stderr_lines.index(error_begin, stderr_cursor)
            error_end_at = stderr_lines.index(error_end, error_begin_at + 1)
        except ValueError as exc:
            raise RuntimeError(
                "missing reference diagnostic marker for statement %d" % index) from exc
        diagnostic = "\n".join(
            stderr_lines[error_begin_at + 1:error_end_at]).strip()
        stderr_cursor = error_end_at + 1
        results.append((rows, state, None, diagnostic, headers))
        cursor = end_at + 1

    return results


def _reference_connection_settings():
    """Resolve the published reference endpoint without exposing credentials."""
    configured = all(os.environ.get(name) for name in
                     ("PGREF_HOST", "PGREF_PORT", "PGREF_PASSWORD"))
    if configured:
        return (os.environ["PGREF_HOST"], int(os.environ["PGREF_PORT"]),
                os.environ.get("PGREF_USER", "postgres"),
                os.environ.get("PGREF_DATABASE", "postgres"),
                os.environ["PGREF_PASSWORD"])

    proc = subprocess.run(
        ["docker", "inspect", CONTAINER], capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError("cannot inspect reference PostgreSQL container: " +
                           proc.stderr.decode().strip())
    try:
        inspected = json.loads(proc.stdout.decode())[0]
        environment = inspected["Config"].get("Env") or []
        password = next(item.split("=", 1)[1] for item in environment
                        if item.startswith("POSTGRES_PASSWORD="))
        bindings = inspected["NetworkSettings"]["Ports"].get("5432/tcp")
        if bindings:
            binding = bindings[0]
            host = binding.get("HostIp") or "127.0.0.1"
            if host in ("0.0.0.0", "::"):
                host = "127.0.0.1"
            port = int(binding["HostPort"])
        else:
            networks = inspected["NetworkSettings"].get("Networks") or {}
            host = next(value["IPAddress"] for value in networks.values()
                        if value.get("IPAddress"))
            port = 5432
    except (KeyError, StopIteration, TypeError, ValueError,
            json.JSONDecodeError) as exc:
        raise RuntimeError(
            "reference PostgreSQL endpoint or password is unavailable") from exc
    return (host, port, os.environ.get("PGREF_USER", "postgres"),
            os.environ.get("PGREF_DATABASE", "postgres"), password)


def decode_wire_result(messages, include_types=False):
    """Decode PostgreSQL protocol messages without altering field bytes."""
    rows = []
    state = None
    message = ""
    headers = []
    type_oids = []
    command_tag = None
    for kind, body in messages:
        if kind == b"T":
            count = int.from_bytes(body[0:2], "big")
            offset = 2
            for _ in range(count):
                end = body.index(b"\x00", offset)
                headers.append(body[offset:end].decode())
                metadata = end + 1
                type_oids.append(int.from_bytes(
                    body[metadata + 6:metadata + 10], "big"))
                offset = metadata + 18
        elif kind == b"D":
            count = int.from_bytes(body[0:2], "big")
            offset = 2
            values = []
            for _ in range(count):
                length = int.from_bytes(
                    body[offset:offset + 4], "big", signed=True)
                offset += 4
                if length == -1:
                    values.append(None)
                else:
                    values.append(body[offset:offset + length].decode())
                    offset += length
            rows.append(values)
        elif kind == b"E":
            for field in body.rstrip(b"\0").split(b"\0"):
                if not field:
                    continue
                tag, value = field[:1].decode(), field[1:].decode()
                if tag == "C":
                    state = value
                elif tag == "M":
                    message = value
        elif kind == b"C":
            command_tag = body.rstrip(b"\0").decode()
    decoded = (rows, state, message, headers, command_tag)
    return decoded + (type_oids,) if include_types else decoded


def reference_multi(statements, client=None):
    """Execute a case through PostgreSQL's wire protocol in one session."""
    if client is None:
        client = load_protocol_client()
    host, port, user, database, password = _reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=15)
    try:
        # The shared DBMS protocol helper validates our exact ParameterStatus
        # contract.  A real PostgreSQL reference legitimately has a different
        # server_version, TimeZone and version-dependent extra fields.
        startup_reference = getattr(client, "startup_reference", client.startup)
        startup_reference(sock, user, database, password=password)
        results = []
        for sql in statements:
            statement = describe_statement(sql)
            decoded = decode_wire_result(
                client.simple_query(sock, statement), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            results.append((rows, state, command_tag, message, headers,
                            type_oids))
        return results
    finally:
        sock.close()


# ------------------------------------------------------------------- our side


def ours_query(client, sock, sql, include_tag=False):
    """Run one statement on this DBMS; return (rows, sqlstate, message)."""
    decoded = decode_wire_result(
        client.simple_query(sock, sql), include_types=include_tag)
    if include_tag:
        rows, state, message, headers, command_tag, type_oids = decoded
        return rows, state, message, headers, command_tag, type_oids
    rows, state, message, headers, command_tag = decoded
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
    ref = reference_multi(stmts, client)
    diffs = []
    compare_headers = os.environ.get("PGDIFF_HEADERS", "1") == "1"

    ours = []
    for sql in stmts:
        response = ours_query(client, sock, sql, include_tag=True)
        if len(response) == 4:  # compatibility with focused unit-test mocks
            rows, state, message, ohead = response
            otag = None
            otypes = None
        elif len(response) == 5:
            rows, state, message, ohead, otag = response
            otypes = None
        else:
            rows, state, message, ohead, otag, otypes = response
        ours.append((rows, state, message, ohead, otag, otypes))

    for sql, reference, (orows, ostate, omsg, ohead, otag, otypes) in zip(stmts, ref, ours):
        rrows, rstate, rtag, rerr = reference[:4]
        rhead = reference[4] if len(reference) > 4 else None
        rtypes = reference[5] if len(reference) > 5 else None
        if normalize_rows(rrows) != normalize_rows(orows):
            diffs.append("%s: rows differ\n  PG:   %r\n  ours: %r" % (sql, rrows, orows))
        if rstate != ostate:
            diffs.append("%s: sqlstate differs: PG=%r ours=%r" % (sql, rstate, ostate))
        if rstate is None and ostate is None and rtag != otag:
            diffs.append("%s: command tag differs: PG=%r ours=%r" %
                         (sql, rtag, otag))
        if (rstate is None and ostate is None and
                rtypes is not None and otypes is not None and
                rtypes != otypes):
            diffs.append("%s: column type OIDs differ: PG=%r ours=%r" %
                         (sql, rtypes, otypes))
        if compare_headers and rstate is None and ostate is None and ohead:
            if rhead is None:
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
        [DBMS_MAIN, "--data-dir", work_dir,
         "--server", str(port), "--insecure"],
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
