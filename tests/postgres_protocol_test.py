#!/usr/bin/env python3
"""Protocol-level regression for PostgreSQL startup, simple and extended query flows."""

import hashlib
import base64
import hmac
import os
import socket
import struct
import subprocess
import tempfile
import time


DBMS_MAIN = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbms_main"))


def frame(body):
    return struct.pack("!I", len(body) + 4) + body


def typed(kind, body=b""):
    return kind + frame(body)


def read_exact(sock, size):
    chunks = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise RuntimeError("connection closed while reading PostgreSQL message")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def read_message(sock):
    kind = read_exact(sock, 1)
    length = struct.unpack("!I", read_exact(sock, 4))[0]
    if length < 4:
        raise RuntimeError("invalid PostgreSQL response length")
    return kind, read_exact(sock, length - 4)


def read_until_ready(sock):
    messages = []
    while True:
        message = read_message(sock)
        messages.append(message)
        if message[0] == b"Z":
            return messages


def scram_verifier(password, salt=b"0123456789abcdef", iterations=4096):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    return (
        "SCRAM-SHA-256$%d:%s$%s:%s"
        % (
            iterations,
            base64.b64encode(salt).decode(),
            base64.b64encode(stored_key).decode(),
            base64.b64encode(server_key).decode(),
        )
    )


def write_auth_catalog(work_dir, username, password, superuser=True):
    catalog_dir = os.path.join(work_dir, "info", "pg_catalog")
    os.makedirs(catalog_dir, exist_ok=True)
    password_record = scram_verifier(password)
    flags = "t,t,t,t,t,f,f" if superuser else "f,t,f,f,t,f,f"
    with open(os.path.join(catalog_dir, "pg_authid.cat"), "w", encoding="utf-8") as auth:
        auth.write('10,"%s",%s,-1,"%s",""\n' %
                   (username, flags, password_record))


def startup(sock, user, database, password="secret", fragmented=False):
    params = b"user\0" + user.encode() + b"\0database\0" + database.encode() + b"\0\0"
    packet = frame(struct.pack("!I", 196608) + params)
    if fragmented:
        sock.sendall(packet[:2])
        time.sleep(0.05)
        sock.sendall(packet[2:])
    else:
        sock.sendall(packet)
    kind, body = read_message(sock)
    assert kind == b"R"
    auth_type = struct.unpack("!I", body[:4])[0]
    if auth_type == 10:
        client_first_bare = "n=%s,r=clientnonce" % user
        initial = b"n,," + client_first_bare.encode()
        sasl_initial = b"SCRAM-SHA-256\0" + struct.pack("!i", len(initial)) + initial
        sock.sendall(typed(b"p", sasl_initial))
        kind, body = read_message(sock)
        assert kind == b"R" and struct.unpack("!I", body[:4])[0] == 11, (kind, body)
        server_first = body[4:].rstrip(b"\0").decode()
        server_attributes = dict(item.split("=", 1) for item in server_first.split(","))
        assert server_attributes["r"].startswith("clientnonce")
        client_final_without_proof = "c=biws,r=" + server_attributes["r"]
        auth_message = client_first_bare + "," + server_first + "," + client_final_without_proof
        salt = base64.b64decode(server_attributes["s"])
        iterations = int(server_attributes["i"])
        salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()
        client_signature = hmac.new(stored_key, auth_message.encode(), hashlib.sha256).digest()
        proof = bytes(left ^ right for left, right in zip(client_key, client_signature))
        sock.sendall(typed(b"p", (client_final_without_proof + ",p=" +
                                  base64.b64encode(proof).decode()).encode()))
        kind, body = read_message(sock)
        assert kind == b"R" and struct.unpack("!I", body[:4])[0] == 12, (kind, body)
        expected_server_signature = base64.b64encode(
            hmac.new(hmac.new(salted, b"Server Key", hashlib.sha256).digest(),
                     auth_message.encode(), hashlib.sha256).digest()
        ).decode()
        assert body[4:].rstrip(b"\0") == ("v=" + expected_server_signature).encode()
    else:
        assert auth_type == 3
        sock.sendall(typed(b"p", password.encode() + b"\0"))
    messages = read_until_ready(sock)
    assert messages[0] == (b"R", struct.pack("!I", 0))
    assert messages[-1] == (b"Z", b"I")
    assert any(kind == b"K" for kind, _ in messages)


def simple_query(sock, sql):
    sock.sendall(typed(b"Q", sql.encode() + b"\0"))
    return read_until_ready(sock)


def extended_query(sock, sql):
    # Parse unnamed statement with no parameter types.
    parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
    sock.sendall(typed(b"P", parse))
    kind, _ = read_message(sock)
    assert kind == b"1"

    # Bind unnamed portal to unnamed statement with text formats and no args.
    bind = b"\0\0" + struct.pack("!H", 0) + struct.pack("!H", 0) + struct.pack("!H", 0)
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"

    execute = b"\0" + struct.pack("!I", 0)
    sock.sendall(typed(b"E", execute))
    sock.sendall(typed(b"S"))
    messages = read_until_ready(sock)
    assert any(kind == b"D" for kind, _ in messages)
    assert any(kind == b"C" for kind, _ in messages)


def main():
    if not os.path.exists(DBMS_MAIN):
        raise SystemExit("run scripts/build.sh first")

    work_dir = tempfile.mkdtemp(prefix="dbms-pg-protocol-")
    process = None
    try:
        os.mkdir(os.path.join(work_dir, "info"))
        open(os.path.join(work_dir, "info", "tlist.lst"), "wb").close()
        write_auth_catalog(work_dir, "alice", "secret")
        with open(os.path.join(work_dir, "pg_hba.conf"), "w", encoding="utf-8") as hba:
            hba.write("host all alice 127.0.0.1/32 scram-sha-256\n"
                      "host all +analyst 127.0.0.1/32 scram-sha-256\n")

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        process = subprocess.Popen(
            [DBMS_MAIN, "--server", str(port), "--insecure"],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        sock = socket.socket()
        sock.settimeout(2)
        deadline = time.time() + 5
        while True:
            try:
                sock.connect(("127.0.0.1", port))
                break
            except OSError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.05)

        startup(sock, "alice", "info")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE ROLE analyst"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE USER bob WITH PASSWORD 'bobpass'"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "GRANT analyst TO bob"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "CREATE TABLE t (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (1)"))
        messages = simple_query(sock, "SELECT id FROM t")
        assert messages[0][0] == b"T"
        assert any(kind == b"D" for kind, _ in messages)
        assert any(kind == b"C" for kind, _ in messages)
        extended_query(sock, "SELECT id FROM t")
        sock.sendall(typed(b"X"))
        sock.close()

        role_sock = socket.socket()
        role_sock.settimeout(2)
        role_sock.connect(("127.0.0.1", port))
        startup(role_sock, "bob", "info", password="bobpass")
        role_sock.sendall(typed(b"X"))
        role_sock.close()

        # In explicit plaintext mode the server must answer the PostgreSQL
        # SSLRequest with 'N' and continue with the normal startup packet.
        plain_sock = socket.socket()
        plain_sock.settimeout(2)
        plain_sock.connect(("127.0.0.1", port))
        plain_sock.sendall(frame(struct.pack("!I", 80877103)))
        assert read_exact(plain_sock, 1) == b"N"
        startup(plain_sock, "alice", "info", fragmented=True)
        plain_sock.sendall(typed(b"X"))
        plain_sock.close()
        print("[PG PROTOCOL] SSLRequest/plaintext negotiation OK")
        print("[PG PROTOCOL] startup/auth/simple/extended query OK")
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for root, dirs, files in os.walk(work_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(work_dir)


if __name__ == "__main__":
    main()
