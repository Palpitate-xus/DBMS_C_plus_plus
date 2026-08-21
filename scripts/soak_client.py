#!/usr/bin/env python3
"""Soak client for DBMS_C_plus_plus (PostgreSQL wire protocol, v3).

Speaks the same startup/SCRAM/simple-query flow as tests/
postgres_protocol_test.py, but as a sustained load generator:

  usage: soak_client.py --host H --port P --user admin --pass admin \
                        --db soakdb --worker ID --duration S [--rate R]

Each worker loops over a mixed INSERT/SELECT/UPDATE/DELETE workload on
its own key range (no cross-worker contention; the interesting axis is
concurrent access to shared buffers/WAL/locks, not logical conflicts)
and prints a one-line JSON summary at the end:

  {"worker": 1, "ops": 1234, "errors": 0, "rows": 118, "seq": 1240}

Errors are counted but not fatal; the harness aggregates and decides.
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import socket
import struct
import sys
import time


# ---------------------------------------------------------------------------
# Wire helpers (subset: startup, SCRAM auth, simple query)
# ---------------------------------------------------------------------------

def send_packet(sock, payload: bytes) -> None:
    sock.sendall(payload)


def recv_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection")
        buf += chunk
    return buf


def recv_message(sock):
    kind = recv_exact(sock, 1)
    (length,) = struct.unpack("!I", recv_exact(sock, 4))
    body = recv_exact(sock, length - 4)
    return kind, body


def build_startup(user: str, database: str) -> bytes:
    params = {"user": user, "database": database, "client_encoding": "UTF8"}
    body = struct.pack("!I", 196608)
    for k, v in params.items():
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack("!I", len(body) + 4) + body


def scram_client_first(sock, user: str, cnonce: str) -> None:
    # SASLInitialResponse: mechanism name, int32 length, initial response.
    client_first_bare = "n=%s,r=%s" % (user, cnonce)
    initial = b"n,," + client_first_bare.encode()
    body = b"SCRAM-SHA-256\x00" + struct.pack("!i", len(initial)) + initial
    send_packet(sock, b"p" + struct.pack("!I", len(body) + 4) + body)


def run_scram(sock, user: str, password: str, cnonce: str) -> None:
    scram_client_first(sock, user, cnonce)

    # AuthenticationSASLContinue carries the server-first-message.
    kind, body = recv_message(sock)
    if kind == b"E":
        raise RuntimeError("auth rejected at SASL continue: %r" % body)
    assert kind == b"R", kind
    (code,) = struct.unpack("!I", body[:4])
    assert code == 11, code
    server_first = body[4:].rstrip(b"\x00").decode()
    attrs = dict(part.split("=", 1) for part in server_first.split(","))
    snonce = attrs["r"]
    salt = base64.b64decode(attrs["s"])
    iterations = int(attrs["i"])
    assert snonce.startswith(cnonce)

    client_first_bare = "n=%s,r=%s" % (user, cnonce)
    client_final_bare = "c=biws,r=" + snonce
    auth_msg = client_first_bare + "," + server_first + "," + client_final_bare
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    sign = hmac.new(stored_key, auth_msg.encode(), hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, sign))
    final = client_final_bare + ",p=" + base64.b64encode(proof).decode()
    send_packet(sock, b"p" + struct.pack("!I", len(final.encode()) + 4)
                + final.encode())

    # AuthenticationSASLFinal (server signature).
    kind, body = recv_message(sock)
    if kind == b"E":
        raise RuntimeError("auth rejected at SASL final: %r" % body)
    assert kind == b"R", kind
    (code,) = struct.unpack("!I", body[:4])
    assert code == 12, code

    # Drain AuthenticationOk / parameter statuses / BackendKeyData until
    # ReadyForQuery.  Ordering of S/K relative to the final R varies, so
    # accept them in any order like the protocol regression does.
    while True:
        kind, body = recv_message(sock)
        if kind == b"Z":
            break
        if kind == b"E":
            raise RuntimeError("post-auth error: %r" % body)
        # S, K, R(0) all fine in any order.
        if kind not in (b"S", b"K", b"R"):
            raise RuntimeError("unexpected post-auth message %r" % kind)


def connect(host: str, port: int, user: str, password: str, database: str):
    sock = socket.create_connection((host, port), timeout=30)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    send_packet(sock, build_startup(user, database))
    kind, body = recv_message(sock)
    if kind == b"E":
        fields = {}
        for part in body.split(b"\x00"):
            if part:
                fields[chr(part[0])] = part[1:].decode(errors="replace")
        raise RuntimeError("startup error: %s/%s: %s" % (
            fields.get("S"), fields.get("C"), fields.get("M")))
    if kind != b"R":
        raise RuntimeError("expected auth request, got %r" % kind)
    (code,) = struct.unpack("!I", body[:4])
    if code == 10:  # SASL
        cnonce = base64.b64encode(os.urandom(18)).decode()
        run_scram(sock, user, password, cnonce)
    elif code == 0:
        pass  # trust
    else:
        raise RuntimeError("unsupported auth code %d" % code)
    return sock


def simple_query(sock, sql: str) -> str:
    payload = b"Q" + struct.pack("!I", len(sql.encode()) + 4 + 1) \
        + sql.encode() + b"\x00"
    send_packet(sock, payload)
    out = []
    while True:
        kind, body = recv_message(sock)
        if kind == b"E":
            fields = {}
            for part in body.split(b"\x00"):
                if part:
                    fields[chr(part[0])] = part[1:].decode(errors="replace")
            out.append("ERROR: " + fields.get("M", "?"))
        if kind == b"Z":
            return "\n".join(out)


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--user", default="admin")
    ap.add_argument("--pass", dest="password", default="admin")
    ap.add_argument("--db", default="soakdb")
    ap.add_argument("--worker", type=int, required=True)
    ap.add_argument("--duration", type=int, required=True)
    ap.add_argument("--rate", type=float, default=0.0,
                    help="max ops/sec per worker (0 = unlimited)")
    args = ap.parse_args()

    worker = args.worker
    deadline = time.time() + args.duration
    ops = errors = seq = 0
    rows_committed = 0
    sock = None
    last_error = ""

    def ensure_conn():
        nonlocal sock
        if sock is None:
            sock = connect(args.host, args.port, args.user,
                           args.password, args.db)

    next_op = 0.0
    while time.time() < deadline:
        if args.rate > 0:
            next_op += 1.0 / args.rate
            delay = next_op - time.time()
            if delay > 0:
                time.sleep(delay)
        seq += 1
        kind = seq % 4
        if kind == 0:
            sql = ("INSERT INTO soak_t VALUES (%d, %d, %d, 'p-%d-%d')"
                   % (worker * 10_000_000 + seq, worker, seq, worker, seq))
        elif kind == 1:
            sql = ("SELECT id, seq FROM soak_t WHERE worker = %d "
                   "AND seq <= %d ORDER BY seq" % (worker, seq))
        elif kind == 2:
            prev = seq - 4 if seq > 4 else seq
            sql = ("UPDATE soak_t SET payload = 'u-%d-%d' "
                   "WHERE worker = %d AND seq = %d" % (worker, seq, worker, prev))
        else:
            prev = seq - 8 if seq > 8 else seq
            sql = ("DELETE FROM soak_t WHERE worker = %d AND seq = %d"
                   % (worker, prev))
        try:
            ensure_conn()
            result = simple_query(sock, sql)
            if "ERROR" in result:
                errors += 1
                last_error = result
                if "does not exist" in result and "soak_t" in result:
                    break
                # Reconnect on next op after an error.
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
            else:
                ops += 1
                if kind == 0:
                    rows_committed += 1
        except (ConnectionError, OSError, RuntimeError, AssertionError) as exc:
            errors += 1
            last_error = str(exc)
            try:
                if sock is not None:
                    sock.close()
            except OSError:
                pass
            sock = None
            time.sleep(0.05)

    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass

    print(json.dumps({
        "worker": worker, "ops": ops, "errors": errors,
        "rows": rows_committed, "seq": seq,
        "last_error": last_error[:200],
    }))
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
