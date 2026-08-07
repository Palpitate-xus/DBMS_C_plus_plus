#!/usr/bin/env python3
"""Protocol-level regression for PostgreSQL startup, simple and extended query flows."""

import hashlib
import base64
import datetime
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


def data_row_values(messages):
    values = []
    for kind, body in messages:
        if kind != b"D":
            continue
        field_count = struct.unpack("!H", body[:2])[0]
        offset = 2
        row = []
        for _ in range(field_count):
            length = struct.unpack("!i", body[offset:offset + 4])[0]
            offset += 4
            if length < 0:
                row.append(None)
            else:
                row.append(body[offset:offset + length])
                offset += length
        values.append(row)
    return values


def row_description_fields(messages):
    for kind, body in messages:
        if kind != b"T":
            continue
        count = struct.unpack("!H", body[:2])[0]
        offset = 2
        fields = []
        for _ in range(count):
            end = body.index(b"\0", offset)
            name = body[offset:end]
            offset = end + 1
            table_oid, attribute_number, type_oid = struct.unpack(
                "!IHI", body[offset:offset + 10])
            offset += 10
            type_size, type_modifier, format_code = struct.unpack(
                "!hiH", body[offset:offset + 8])
            offset += 8
            fields.append((name, table_oid, attribute_number, type_oid,
                           type_size, type_modifier, format_code))
        return fields
    raise AssertionError("RowDescription was not returned")


def extended_query(sock, sql):
    # Parse unnamed statement with no parameter types.
    parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 0), (kind, body)
    kind, _ = read_message(sock)
    assert kind == b"1"

    sock.sendall(typed(b"D", b"S\0"))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 0)
    kind, _ = read_message(sock)
    assert kind == b"n"

    # Bind unnamed portal to unnamed statement with text formats and no args.
    bind = b"\0\0" + struct.pack("!H", 0) + struct.pack("!H", 0) + struct.pack("!H", 0)
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"

    execute = b"\0" + struct.pack("!I", 0)
    sock.sendall(typed(b"E", execute))
    sock.sendall(typed(b"S"))
    messages = read_until_ready(sock)
    fields = row_description_fields(messages)
    assert len(fields) == 1 and fields[0][0] == b"id", fields
    assert fields[0][1] != 0 and fields[0][2:] == (1, 23, 4, -1, 0), fields
    assert any(kind == b"D" for kind, _ in messages)
    assert any(kind == b"C" for kind, _ in messages)

    sock.sendall(typed(b"C", b"P\0") + typed(b"C", b"S\0") + typed(b"S"))
    messages = read_until_ready(sock)
    assert sum(kind == b"3" for kind, _ in messages) == 2, messages


def extended_query_int_parameter(sock, sql, value):
    parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 1) + struct.pack("!I", 23)
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 1) + struct.pack("!I", 23)
    kind, _ = read_message(sock)
    assert kind == b"1"

    encoded = str(value).encode()
    bind = (b"\0\0" + struct.pack("!H", 0) + struct.pack("!H", 1) +
            struct.pack("!i", len(encoded)) + encoded + struct.pack("!H", 0))
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"
    sock.sendall(typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = read_until_ready(sock)
    assert data_row_values(messages) == [[str(value).encode()]], messages
    assert any(kind == b"C" for kind, _ in messages)


def extended_query_binary_int_parameter(sock, sql, value):
    parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 1) + struct.pack("!I", 23)
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 1) + struct.pack("!I", 23)
    kind, _ = read_message(sock)
    assert kind == b"1"

    bind = (b"\0\0" + struct.pack("!H", 1) + struct.pack("!H", 1) +
            struct.pack("!H", 1) + struct.pack("!i", 4) + struct.pack("!i", value) +
            struct.pack("!H", 1) + struct.pack("!H", 1))
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"
    sock.sendall(typed(b"E", b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = read_until_ready(sock)
    fields = row_description_fields(messages)
    assert len(fields) == 1 and fields[0][3] == 23 and fields[0][6] == 1, fields
    assert data_row_values(messages) == [[struct.pack("!i", value)]], messages
    assert any(kind == b"C" for kind, _ in messages)


def extended_query_binary_parameter(sock, suffix, sql, type_oid, raw, expected):
    statement_name = ("binary_" + suffix).encode()
    portal_name = ("binary_portal_" + suffix).encode()
    parse = (statement_name + b"\0" + sql.encode() + b"\0" +
             struct.pack("!H", 1) + struct.pack("!I", type_oid))
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 1) + struct.pack("!I", type_oid)
    kind, _ = read_message(sock)
    assert kind == b"1"

    bind = (portal_name + b"\0" + statement_name + b"\0" +
            struct.pack("!H", 1) + struct.pack("!H", 1) +
            struct.pack("!H", 1) + struct.pack("!i", len(raw)) + raw +
            struct.pack("!H", 1) + struct.pack("!H", 1))
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"
    sock.sendall(typed(b"E", portal_name + b"\0" + struct.pack("!I", 0)) + typed(b"S"))
    messages = read_until_ready(sock)
    fields = row_description_fields(messages)
    assert len(fields) == 1 and fields[0][3] == type_oid and fields[0][6] == 1, fields
    assert data_row_values(messages) == [[expected]], messages
    assert any(kind == b"C" for kind, _ in messages)

    sock.sendall(typed(b"C", b"P" + portal_name + b"\0") +
                 typed(b"C", b"S" + statement_name + b"\0") + typed(b"S"))
    messages = read_until_ready(sock)
    assert sum(kind == b"3" for kind, _ in messages) == 2, messages


def extended_query_temporal_binary_parameters(sock):
    epoch_date = datetime.date(2000, 1, 1)
    sample_date = datetime.date(2026, 8, 7)
    date_days = (sample_date - epoch_date).days
    epoch_timestamp = datetime.datetime(2000, 1, 1)
    sample_timestamp = datetime.datetime(2026, 8, 7, 12, 34, 56)
    timestamp_micros = int((sample_timestamp - epoch_timestamp).total_seconds() * 1000000)
    time_micros = (12 * 3600 + 34 * 60 + 56) * 1000000
    uuid_bytes = bytes.fromhex("550e8400e29b41d4a716446655440000")

    extended_query_binary_parameter(
        sock, "date", "SELECT d FROM protocol_temporal WHERE d = $1", 1082,
        struct.pack("!i", date_days), struct.pack("!i", date_days))
    extended_query_binary_parameter(
        sock, "time", "SELECT tm FROM protocol_temporal WHERE tm = $1", 1083,
        struct.pack("!q", time_micros), struct.pack("!q", time_micros))
    extended_query_binary_parameter(
        sock, "timestamp", "SELECT ts FROM protocol_temporal WHERE ts = $1", 1114,
        struct.pack("!q", timestamp_micros), struct.pack("!q", timestamp_micros))
    extended_query_binary_parameter(
        sock, "timestamptz", "SELECT tz FROM protocol_temporal WHERE tz = $1", 1184,
        struct.pack("!q", timestamp_micros), struct.pack("!q", timestamp_micros))
    extended_query_binary_parameter(
        sock, "uuid", "SELECT u FROM protocol_temporal WHERE u = $1", 2950,
        uuid_bytes, uuid_bytes)


def extended_query_numeric_binary_parameter(sock):
    numeric_raw = struct.pack("!hhhhHHH", 3, 1, 0, 2, 1, 2345, 6700)
    extended_query_binary_parameter(
        sock, "numeric", "SELECT n FROM protocol_numeric WHERE n = $1", 1700,
        numeric_raw, numeric_raw)


def extended_query_portal_pagination(sock):
    parse = b"paged_stmt\0SELECT id FROM portal_t\0" + struct.pack("!H", 0)
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"t" and body == struct.pack("!H", 0)
    kind, _ = read_message(sock)
    assert kind == b"1"

    bind = b"paged\0paged_stmt\0" + struct.pack("!H", 0) + struct.pack("!H", 0) + struct.pack("!H", 0)
    sock.sendall(typed(b"B", bind))
    kind, _ = read_message(sock)
    assert kind == b"2"

    def execute_batch(expected_rows, suspended):
        sock.sendall(typed(b"E", b"paged\0" + struct.pack("!I", 1)) + typed(b"S"))
        messages = read_until_ready(sock)
        assert data_row_values(messages) == expected_rows, messages
        assert any(kind == b"s" for kind, _ in messages) if suspended else not any(kind == b"s" for kind, _ in messages), messages
        assert not any(kind == b"C" for kind, _ in messages) if suspended else any(kind == b"C" for kind, _ in messages), messages
        return messages

    execute_batch([[b"1"]], True)
    execute_batch([[b"3"]], False)
    sock.sendall(typed(b"E", b"paged\0" + struct.pack("!I", 1)) + typed(b"S"))
    messages = read_until_ready(sock)
    assert data_row_values(messages) == []
    assert any(kind == b"C" for kind, _ in messages)

    sock.sendall(typed(b"C", b"P\0") + typed(b"C", b"S\0") + typed(b"S"))
    messages = read_until_ready(sock)
    assert sum(kind == b"3" for kind, _ in messages) == 2, messages


def extended_query_error_recovery(sock):
    # A Parse error puts the extended-query protocol into the ignore-until-Sync
    # state. Bind/Execute sent before Sync must not run or emit completions.
    parse = b"\0SELECT 1\0" + struct.pack("!H", 1)
    bind = b"\0\0" + struct.pack("!H", 0) + struct.pack("!H", 0) + struct.pack("!H", 0)
    execute = b"\0" + struct.pack("!I", 0)
    sock.sendall(typed(b"P", parse) + typed(b"B", bind) + typed(b"E", execute) + typed(b"S"))
    messages = read_until_ready(sock)
    assert sum(kind == b"E" for kind, _ in messages) == 1
    assert not any(kind in (b"1", b"2", b"C", b"D") for kind, _ in messages)
    assert messages[-1] == (b"Z", b"I")

    # The connection is usable again after Sync.
    messages = simple_query(sock, "SELECT id FROM t")
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
            sock, "CREATE USER bob WITH PASSWORD 'bObPass9!'"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "GRANT analyst TO bob"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "CREATE TABLE t (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "GRANT SELECT ON t TO analyst"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE portal_t (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO portal_t VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO portal_t VALUES (3)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_temporal "
            "(d DATE, tm TIME, ts TIMESTAMP, tz TIMESTAMPTZ, u UUID)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO protocol_temporal VALUES "
            "('2026-08-07', '12:34:56', '2026-08-07 12:34:56', "
            "'2026-08-07 12:34:56+00:00', "
            "'550e8400-e29b-41d4-a716-446655440000')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_numeric (n NUMERIC)"))
        numeric_description = row_description_fields(
            simple_query(sock, "SELECT n FROM protocol_numeric"))
        assert numeric_description[0][3] == 1700, numeric_description
        assert numeric_description[0][4] == -1, numeric_description
        numeric_insert_messages = simple_query(
            sock, "INSERT INTO protocol_numeric VALUES ('12345.67')")
        assert any(kind == b"C" for kind, _ in numeric_insert_messages), numeric_insert_messages
        assert any(kind == b"C" and body == b"INSERT 0 1\0"
                   for kind, body in numeric_insert_messages), numeric_insert_messages
        numeric_messages = simple_query(sock, "SELECT n FROM protocol_numeric")
        numeric_rows = data_row_values(numeric_messages)
        assert numeric_rows == [[b"12345.67"]], numeric_messages
        numeric_rows = data_row_values(simple_query(
            sock, "SELECT n FROM protocol_numeric WHERE n = 12345.67"))
        assert numeric_rows == [[b"12345.67"]], numeric_rows
        messages = simple_query(sock, "SELECT id FROM t")
        assert messages[0][0] == b"T"
        assert any(kind == b"D" for kind, _ in messages)
        assert any(kind == b"C" for kind, _ in messages)
        extended_query(sock, "SELECT id FROM t")
        extended_query_int_parameter(sock, "SELECT id FROM t WHERE id = $1", 1)
        extended_query_binary_int_parameter(sock, "SELECT id FROM t WHERE id = $1", 1)
        extended_query_temporal_binary_parameters(sock)
        extended_query_numeric_binary_parameter(sock)
        extended_query_portal_pagination(sock)
        extended_query_error_recovery(sock)
        error_messages = simple_query(sock, "SELECT * FROM protocol_missing_table")
        assert any(kind == b"E" for kind, _ in error_messages)
        assert error_messages[-1] == (b"Z", b"I")
        assert any(kind == b"C" for kind, _ in simple_query(sock, "SELECT id FROM t"))

        # A backend's transaction state must not leak through the shared
        # StorageEngine into another protocol connection.
        peer_sock = socket.socket()
        peer_sock.settimeout(2)
        peer_sock.connect(("127.0.0.1", port))
        startup(peer_sock, "alice", "info")
        assert simple_query(peer_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (2)"))
        peer_rows = data_row_values(simple_query(peer_sock, "SELECT id FROM t"))
        assert peer_rows == [[b"1"]], peer_rows
        own_rows = data_row_values(simple_query(sock, "SELECT id FROM t"))
        assert own_rows == [[b"1"], [b"2"]], own_rows
        assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(peer_sock, "SELECT id FROM t")) == [[b"1"]]
        assert simple_query(peer_sock, "COMMIT")[-1] == (b"Z", b"I")
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (3)"))
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        assert simple_query(peer_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert data_row_values(simple_query(peer_sock, "SELECT id FROM t")) == [[b"1"], [b"3"]]
        assert simple_query(peer_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        peer_sock.sendall(typed(b"X"))
        peer_sock.close()

        # Disconnecting a backend must abort its open transaction and discard
        # its context before the worker thread can be reused.
        leaked_sock = socket.socket()
        leaked_sock.settimeout(2)
        leaked_sock.connect(("127.0.0.1", port))
        startup(leaked_sock, "alice", "info")
        assert simple_query(leaked_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(leaked_sock, "INSERT INTO t VALUES (4)"))
        leaked_sock.sendall(typed(b"X"))
        leaked_sock.close()
        time.sleep(0.1)

        observer_sock = socket.socket()
        observer_sock.settimeout(2)
        observer_sock.connect(("127.0.0.1", port))
        startup(observer_sock, "alice", "info")
        assert simple_query(observer_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert data_row_values(simple_query(observer_sock, "SELECT id FROM t")) == [[b"1"], [b"3"]]
        assert simple_query(observer_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        observer_sock.sendall(typed(b"X"))
        observer_sock.close()
        sock.sendall(typed(b"X"))
        sock.close()

        role_sock = socket.socket()
        role_sock.settimeout(2)
        role_sock.connect(("127.0.0.1", port))
        startup(role_sock, "bob", "info", password="bObPass9!")
        role_rows = data_row_values(simple_query(role_sock, "SELECT id FROM t"))
        assert role_rows == [[b"1"], [b"3"]], role_rows
        denied_rows = simple_query(role_sock, "INSERT INTO t VALUES (99)")
        assert any(kind == b"E" for kind, _ in denied_rows)
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
        print("[PG PROTOCOL] extended-query error recovery and ReadyForQuery status OK")
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
