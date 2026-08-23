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
SOCKET_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_TEST_TIMEOUT", "10"))
STARTUP_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_STARTUP_TIMEOUT", "15"))


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


def setting_value(messages, name):
    for row in data_row_values(messages):
        if row and row[0] == name.encode():
            return row[1]
    raise AssertionError("setting %s was not returned" % name)


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


def transaction_error_state_recovery(sock):
    # An error inside an explicit transaction must make the transaction
    # rollback-only. COMMIT is PostgreSQL-compatible shorthand for rollback in
    # this state; it must not publish the earlier INSERT.
    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "INSERT INTO t VALUES (41)"))
    failed = simple_query(sock, "INSERT INTO t VALUES (41, 42)")
    assert any(kind == b"E" for kind, _ in failed), failed
    assert failed[-1] == (b"Z", b"E"), failed
    aborted = simple_query(sock, "SELECT id FROM t WHERE id = 41")
    assert any(kind == b"E" for kind, _ in aborted), aborted
    assert aborted[-1] == (b"Z", b"E"), aborted
    committed = simple_query(sock, "COMMIT")
    assert committed[-1] == (b"Z", b"I"), committed
    assert data_row_values(simple_query(sock, "SELECT id FROM t WHERE id = 41")) == []

    # ROLLBACK TO SAVEPOINT is also a legal recovery command and clears the
    # failed state while retaining the surrounding transaction.
    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(sock, "SAVEPOINT tx_error"))
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "INSERT INTO t VALUES (42)"))
    failed = simple_query(sock, "INSERT INTO t VALUES (42, 43)")
    assert any(kind == b"E" for kind, _ in failed)
    recovered = simple_query(sock, "ROLLBACK TO SAVEPOINT tx_error")
    assert recovered[-1] == (b"Z", b"T"), recovered
    assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
    assert data_row_values(simple_query(sock, "SELECT id FROM t WHERE id = 42")) == []


def prepared_transaction_error_boundaries(sock):
    # Two-phase completion commands are not substitutes for local transaction
    # recovery. They must remain rejected while the backend is in 25P02.
    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    failed = simple_query(sock, "INSERT INTO t VALUES (43, 44)")
    assert any(kind == b"E" for kind, _ in failed), failed
    prepared = simple_query(sock, "COMMIT PREPARED 'missing_protocol_xid'")
    assert any(kind == b"E" for kind, _ in prepared), prepared
    assert prepared[-1] == (b"Z", b"E"), prepared
    assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")

    # Outside a transaction, an unknown prepared transaction is an execution
    # error rather than a successful command with informational output.
    missing = simple_query(sock, "ROLLBACK PREPARED 'missing_protocol_xid'")
    assert any(kind == b"E" for kind, _ in missing), missing
    assert missing[-1] == (b"Z", b"I"), missing


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
            # The protocol test deliberately drives many commands.  Do not
            # leave a child stdout/stderr PIPE unread: once its buffer fills,
            # the server blocks while the client waits for ReadyForQuery.
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        sock = socket.socket()
        sock.settimeout(SOCKET_TIMEOUT)
        deadline = time.time() + STARTUP_TIMEOUT
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
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "GRANT analyst TO bob WITH ADMIN OPTION"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE USER carol WITH PASSWORD 'cArolPass9!'"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "CREATE TABLE t (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT SELECT ON TABLES TO analyst"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE default_acl_protocol (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO default_acl_protocol VALUES (7)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_truncate (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO protocol_truncate VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "TRUNCATE TABLE protocol_truncate"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_truncate")) == []
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE dml_ast (id INT DEFAULT 7, name TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dml_ast VALUES (DEFAULT, 'first'), (2, NULL), (NULL, 'null-id')"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_ast")) == [[b"7"], [b"2"], [b"0"]]
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dml_ast DEFAULT VALUES"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_ast WHERE id = 7")) == [[b"7"], [b"7"]]
        returning_default = simple_query(
            sock, "INSERT INTO dml_ast DEFAULT VALUES RETURNING id, name")
        assert data_row_values(returning_default) == [[b"7", None]], returning_default
        assert any(kind == b"C" and body == b"INSERT 0 1\0"
                   for kind, body in returning_default), returning_default
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dml_ast VALUES (3, 'MiXeD')"))
        assert data_row_values(simple_query(
            sock, "SELECT name FROM dml_ast WHERE id = 3")) == [[b"MiXeD"]]
        returning_insert = simple_query(
            sock, "INSERT INTO dml_ast VALUES (4, 'returned'), (5, 'returned2') "
            "RETURNING id, name")
        assert data_row_values(returning_insert) == [
            [b"4", b"returned"], [b"5", b"returned2"]], returning_insert
        assert any(kind == b"C" and body == b"INSERT 0 2\0"
                   for kind, body in returning_insert), returning_insert
        returning_expr = simple_query(
            sock, "INSERT INTO dml_ast VALUES (6, 'expr') "
            "RETURNING id + 10 AS next_id, name || '-x' AS tagged")
        assert data_row_values(returning_expr) == [[b"16", b"expr-x"]], returning_expr
        returning_expr_fields = row_description_fields(returning_expr)
        assert [field[0] for field in returning_expr_fields] == [
            b"next_id", b"tagged"], returning_expr
        assert [field[3] for field in returning_expr_fields] == [23, 25], returning_expr
        assert any(kind == b"C" and body == b"INSERT 0 1\0"
                   for kind, body in returning_expr), returning_expr
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE dml_copy (id INT, name TEXT)"))
        returning_select = simple_query(
            sock, "INSERT INTO dml_copy (id, name) "
            "SELECT id, name FROM dml_ast WHERE id = 3 RETURNING id, name")
        assert data_row_values(returning_select) == [[b"3", b"MiXeD"]], returning_select
        assert any(kind == b"C" and body == b"INSERT 0 1\0"
                   for kind, body in returning_select), returning_select
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE dml_conflict (id INT PRIMARY KEY, name TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'old')"))
        atomic_failure = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (9, 'rolled-back'), (1, 'duplicate')")
        assert any(kind == b"E" for kind, _ in atomic_failure), atomic_failure
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_conflict WHERE id = 9")) == [], atomic_failure
        cte_atomic_failure = simple_query(
            sock, "WITH attempted AS (INSERT INTO dml_conflict VALUES "
            "(10, 'cte-rolled-back'), (1, 'duplicate') RETURNING id) "
            "SELECT id FROM attempted")
        assert any(kind == b"E" for kind, _ in cte_atomic_failure), cte_atomic_failure
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_conflict WHERE id = 10")) == [], cte_atomic_failure
        cte_success = simple_query(
            sock, "WITH inserted AS (INSERT INTO dml_conflict VALUES "
            "(11, 'cte-committed') RETURNING id) SELECT id FROM inserted")
        assert data_row_values(cte_success) == [[b"11"]], cte_success
        conflict_messages = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'ignored'), (2, 'new') "
            "ON CONFLICT DO NOTHING RETURNING id, name")
        assert data_row_values(conflict_messages) == [[b"2", b"new"]], conflict_messages
        assert any(kind == b"C" and body == b"INSERT 0 1\0"
                   for kind, body in conflict_messages), conflict_messages
        upsert_messages = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'ignored'), (3, 'third') "
            "ON CONFLICT (id) DO UPDATE SET name = 'upserted' "
            "RETURNING id, name")
        assert data_row_values(upsert_messages) == [
            [b"1", b"upserted"], [b"3", b"third"]], upsert_messages
        assert any(kind == b"C" and body == b"INSERT 0 2\0"
                   for kind, body in upsert_messages), upsert_messages
        excluded_upsert = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'excluded-update'), (4, 'four') "
            "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
            "RETURNING id, name")
        assert data_row_values(excluded_upsert) == [
            [b"1", b"excluded-update"], [b"4", b"four"]], excluded_upsert
        assert any(kind == b"C" and body == b"INSERT 0 2\0"
                   for kind, body in excluded_upsert), excluded_upsert
        excluded_expr_upsert = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'expression'), (5, 'five') "
            "ON CONFLICT (id) DO UPDATE SET name = excluded.name || '-x' "
            "RETURNING id, name")
        assert data_row_values(excluded_expr_upsert) == [
            [b"1", b"expression-x"], [b"5", b"five"]], excluded_expr_upsert
        assert any(kind == b"C" and body == b"INSERT 0 2\0"
                   for kind, body in excluded_expr_upsert), excluded_expr_upsert
        conflict_where = simple_query(
            sock, "INSERT INTO dml_conflict VALUES (1, 'where-update'), (6, 'six') "
            "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
            "WHERE name = 'expression-x' RETURNING id, name")
        assert data_row_values(conflict_where) == [
            [b"1", b"where-update"], [b"6", b"six"]], conflict_where
        assert any(kind == b"C" and body == b"INSERT 0 2\0"
                   for kind, body in conflict_where), conflict_where
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UPDATE dml_ast SET name = 'changed' WHERE id = 2"))
        assert data_row_values(simple_query(
            sock, "SELECT name FROM dml_ast WHERE id = 2")) == [[b"changed"]]
        returning_update = simple_query(
            sock, "UPDATE dml_ast SET id = 8 WHERE id = 2 RETURNING id, name")
        assert data_row_values(returning_update) == [[b"8", b"changed"]], returning_update
        assert [field[0] for field in row_description_fields(returning_update)] == [b"id", b"name"]
        assert any(kind == b"C" and body == b"UPDATE 1\0"
                   for kind, body in returning_update), returning_update
        returning_update_expr = simple_query(
            sock, "UPDATE dml_ast SET name = 'changed2' WHERE id = 8 "
            "RETURNING id + 10 AS next_id, name || '-x' AS tagged")
        assert data_row_values(returning_update_expr) == [[b"18", b"changed2-x"]], returning_update_expr
        assert any(kind == b"C" and body == b"UPDATE 1\0"
                   for kind, body in returning_update_expr), returning_update_expr
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "DELETE FROM dml_ast WHERE id = 3"))
        returning_delete = simple_query(
            sock, "DELETE FROM dml_ast WHERE id = 8 RETURNING *")
        assert data_row_values(returning_delete) == [[b"8", b"changed2"]], returning_delete
        assert any(kind == b"C" and body == b"DELETE 1\0"
                   for kind, body in returning_delete), returning_delete
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_ast WHERE id = 3")) == []
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE INDEX t_id_idx ON t(id)"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM t WHERE id = 1")) == [[b"1"]]
        database_stats = data_row_values(simple_query(
            sock, "SELECT * FROM pg_catalog.pg_stat_database"))
        info_stats = next((row for row in database_stats if row and row[0] == b"info"), None)
        assert info_stats is not None and len(info_stats) == 7, database_stats
        assert int(info_stats[4]) >= 0, info_stats
        table_stats = data_row_values(simple_query(
            sock, "SELECT * FROM pg_catalog.pg_stat_tables"))
        table_row = next((row for row in table_stats if row and row[0] == b"t"), None)
        assert table_row is not None and len(table_row) == 9, table_stats
        assert int(table_row[3]) >= 1 and int(table_row[4]) >= 1, table_row
        assert int(table_row[5]) >= 1 and int(table_row[8]) >= 1, table_row
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE aggregate_t (id INT, amount INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO aggregate_t VALUES (1, 10), (2, 20), (3, 30)"))
        aggregate_rows = data_row_values(simple_query(
            sock, "SELECT count(*), sum(amount) FROM aggregate_t WHERE amount > 10"))
        assert aggregate_rows == [[b"2", b"50"]], aggregate_rows
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE sub_outer (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE sub_inner (id INT, enabled INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO sub_outer VALUES (1), (2), (3), (4)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO sub_inner VALUES (2, 1), (3, 1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO sub_inner (enabled) VALUES (1)"))
        semi_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id IN (SELECT id FROM sub_inner WHERE enabled = 1)"))
        assert semi_rows == [[b"2"], [b"3"]], semi_rows
        anti_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id NOT IN (SELECT id FROM sub_inner WHERE enabled = 1)"))
        assert anti_rows == [], anti_rows
        exists_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE EXISTS (SELECT 1 FROM sub_inner WHERE enabled = 1)"))
        assert exists_rows == [[b"1"], [b"2"], [b"3"], [b"4"]], exists_rows
        not_exists_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE NOT EXISTS (SELECT 1 FROM sub_inner WHERE enabled = 9)"))
        assert not_exists_rows == [[b"1"], [b"2"], [b"3"], [b"4"]], not_exists_rows
        not_exists_hit_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE NOT EXISTS (SELECT 1 FROM sub_inner WHERE enabled = 1)"))
        assert not_exists_hit_rows == [], not_exists_hit_rows
        any_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id > ANY (SELECT id FROM sub_inner WHERE enabled = 1)"))
        assert any_rows == [[b"3"], [b"4"]], any_rows
        all_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id > ALL (SELECT id FROM sub_inner WHERE enabled = 1)"))
        assert all_rows == [], all_rows  # NULL inner value makes ALL UNKNOWN.
        quantified_explain = data_row_values(simple_query(
            sock, "EXPLAIN SELECT id FROM sub_outer "
            "WHERE id > ANY (SELECT id FROM sub_inner WHERE enabled = 1)"))
        assert any(b"QuantifiedSubqueryFilter" in row[0] for row in quantified_explain), quantified_explain
        nonnull_all_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id > ALL (SELECT id FROM sub_inner "
            "WHERE enabled = 1 AND id IS NOT NULL)"))
        assert nonnull_all_rows == [[b"4"]], nonnull_all_rows
        empty_any_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id > ANY (SELECT id FROM sub_inner WHERE enabled = 9)"))
        assert empty_any_rows == [], empty_any_rows
        empty_all_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE id > ALL (SELECT id FROM sub_inner WHERE enabled = 9)"))
        assert empty_all_rows == [[b"1"], [b"2"], [b"3"], [b"4"]], empty_all_rows
        scalar_rows = data_row_values(simple_query(
            sock, "SELECT id, (SELECT id FROM sub_inner WHERE id = 2) "
            "FROM sub_outer"))
        assert scalar_rows == [[b"1", b"2"], [b"2", b"2"],
                               [b"3", b"2"], [b"4", b"2"]], scalar_rows
        scalar_null_rows = data_row_values(simple_query(
            sock, "SELECT id, (SELECT id FROM sub_inner WHERE id = 9) "
            "FROM sub_outer"))
        assert scalar_null_rows == [[b"1", None], [b"2", None],
                                    [b"3", None], [b"4", None]], scalar_null_rows
        scalar_multi_messages = simple_query(
            sock, "SELECT id, (SELECT id FROM sub_inner WHERE enabled = 1) "
            "FROM sub_outer")
        assert any(kind == b"E" for kind, _ in scalar_multi_messages), scalar_multi_messages
        assert any(kind == b"E" and b"C21000\x00" in body
                   for kind, body in scalar_multi_messages), scalar_multi_messages
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "GRANT SELECT ON t TO analyst WITH GRANT OPTION"))
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

        # Set operations use one shared execution path.  Exercise duplicate
        # elimination, multiset ALL semantics, and INTERSECT precedence.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE set_left (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE set_right (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO set_left VALUES (1), (1), (2), (3)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO set_right VALUES (1), (2), (2), (4)"))
        union_messages = simple_query(
            sock, "SELECT id FROM set_left UNION SELECT id FROM set_right")
        assert data_row_values(union_messages) == [
                [b"1"], [b"2"], [b"3"], [b"4"]], union_messages
        assert data_row_values(simple_query(
            sock, "SELECT id FROM set_left UNION ALL SELECT id FROM set_right")) == [
                [b"1"], [b"1"], [b"2"], [b"3"], [b"1"], [b"2"], [b"2"], [b"4"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM set_left INTERSECT SELECT id FROM set_right")) == [
                [b"1"], [b"2"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM set_left INTERSECT ALL SELECT id FROM set_right")) == [
                [b"1"], [b"2"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM set_left EXCEPT SELECT id FROM set_right")) == [[b"3"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM set_left EXCEPT ALL SELECT id FROM set_right")) == [
                [b"1"], [b"3"]]
        # A complex operand may still use the legacy SELECT producer, but the
        # set semantics must be executed by the same structured SetOperationOp.
        complex_set_rows = data_row_values(simple_query(
            sock, "SELECT id FROM set_left UNION SELECT count(*) FROM set_right"))
        assert complex_set_rows == [[b"1"], [b"2"], [b"3"], [b"4"]], complex_set_rows

        # OR branches with separate equality indexes use one BitmapOr heap
        # fetch path and must preserve all matching rows.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE bitmap_t (id INT, tenant INT, state INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE INDEX ON bitmap_t(tenant)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE INDEX ON bitmap_t(state)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO bitmap_t VALUES "
            "(1, 7, 1), (2, 7, 2), (3, 8, 1)"))
        bitmap_or_rows = data_row_values(simple_query(
            sock, "SELECT id FROM bitmap_t WHERE tenant = 7 OR state = 1"))
        assert bitmap_or_rows == [[b"1"], [b"2"], [b"3"]], bitmap_or_rows

        # Join-view INSTEAD OF triggers: the view has no single BASE_TABLE,
        # so row collection must go through the view's own SELECT.  A join
        # view must be selectable, and UPDATE/DELETE through it must fire
        # the trigger per visible row (regression: both silently affected
        # zero rows when only BASE_TABLE views were collected).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE jt_a (aid INT PRIMARY KEY, tag TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE jt_b (bid INT PRIMARY KEY, aid INT, val TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO jt_a VALUES (1, 'x')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO jt_b VALUES (10, 1, 'v1')"))
        # Stored view SQL keeps qualified refs executable (jb.bid, not
        # "jb . bid" which the join executor cannot resolve).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE VIEW jt_view AS "
            "SELECT jt_b.bid, jt_a.tag, jt_b.val FROM jt_b "
            "JOIN jt_a ON jt_b.aid = jt_a.aid"))
        join_rows = data_row_values(simple_query(
            sock, "SELECT bid, tag, val FROM jt_view"))
        assert join_rows == [[b"10", b"x", b"v1"]], join_rows
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER jt_view_update INSTEAD OF UPDATE ON jt_view "
            "FOR EACH ROW UPDATE jt_b SET val = NEW.val WHERE bid = OLD.bid"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UPDATE jt_view SET val = 'v2' WHERE bid = 10"))
        updated = data_row_values(simple_query(
            sock, "SELECT val FROM jt_b WHERE bid = 10"))
        assert updated == [[b"v2"]], updated
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER jt_view_delete INSTEAD OF DELETE ON jt_view "
            "FOR EACH ROW DELETE FROM jt_b WHERE bid = OLD.bid"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "DELETE FROM jt_view WHERE bid = 10"))
        remaining = data_row_values(simple_query(
            sock, "SELECT bid FROM jt_b WHERE bid = 10"))
        assert remaining == [], remaining

        # INSTEAD OF view triggers must be creatable on views and must route
        # each DML operation to the trigger action instead of the base-view
        # rewrite path.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE view_trigger_base (id INT PRIMARY KEY, name TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE VIEW writable_view AS "
            "SELECT id, name FROM view_trigger_base"))
        insert_trigger_ddl = simple_query(
            sock, "CREATE TRIGGER writable_view_insert INSTEAD OF INSERT ON writable_view "
            "FOR EACH ROW INSERT INTO view_trigger_base VALUES (NEW.id, NEW.name)")
        assert any(kind == b"C" for kind, _ in insert_trigger_ddl), insert_trigger_ddl
        update_trigger_ddl = simple_query(
            sock, "CREATE TRIGGER writable_view_update INSTEAD OF UPDATE ON writable_view "
            "FOR EACH ROW UPDATE view_trigger_base SET name = NEW.name WHERE id = OLD.id")
        assert any(kind == b"C" for kind, _ in update_trigger_ddl), update_trigger_ddl
        delete_trigger_ddl = simple_query(
            sock, "CREATE TRIGGER writable_view_delete INSTEAD OF DELETE ON writable_view "
            "FOR EACH ROW DELETE FROM view_trigger_base WHERE id = OLD.id")
        assert any(kind == b"C" for kind, _ in delete_trigger_ddl), delete_trigger_ddl
        insert_messages = simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES "
            "(10, 'alice'), (11, 'carol')")
        assert any(kind == b"C" for kind, _ in insert_messages), insert_messages
        view_rows = simple_query(sock, "SELECT name FROM view_trigger_base WHERE id = 10")
        assert data_row_values(view_rows) == [[b"alice"]], view_rows
        assert data_row_values(simple_query(
            sock, "SELECT name FROM view_trigger_base WHERE id = 11")) == [[b"carol"]]
        # RETURNING through an INSTEAD OF insert trigger emits the projected
        # NEW values as real RowData messages (regression: command tag only,
        # zero rows).
        returning_rows = data_row_values(simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES (50, 'ret') "
            "RETURNING id, name"))
        assert returning_rows == [[b"50", b"ret"]], returning_rows

        # Transition tables: REFERENCING OLD TABLE AS <alias> on a
        # statement-level trigger exposes the pre-statement row set to the
        # action SQL as a queryable table.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE tt_src (id INT PRIMARY KEY, v TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO tt_src VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE tt_archive (id INT, v TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER tt_del_arch AFTER DELETE ON tt_src "
            "REFERENCING OLD TABLE AS deleted_rows FOR EACH STATEMENT "
            "INSERT INTO tt_archive SELECT id, v FROM deleted_rows"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "DELETE FROM tt_src WHERE id <= 2"))
        tt_arch = data_row_values(simple_query(
            sock, "SELECT id, v FROM tt_archive"))
        assert tt_arch == [[b"1", b"a"], [b"2", b"b"]], tt_arch
        tt_left = data_row_values(simple_query(
            sock, "SELECT id FROM tt_src"))
        assert tt_left == [[b"3"]], tt_left
        # The transition alias is dropped after the statement.
        tt_gone = simple_query(sock, "SELECT * FROM deleted_rows")
        assert any(kind == b"E" for kind, _ in tt_gone), tt_gone

        # "use <db>" must not kill the backend: the parser classifies any
        # leading "use" as UseDatabase and the handler used to slice a
        # 13-char prefix unconditionally (out_of_range abort on the short
        # form).  Both forms now succeed/fail cleanly and the connection
        # stays alive afterwards.
        use_short = simple_query(sock, "use info")
        assert any(kind == b"Z" for kind, _ in use_short), use_short
        use_long = simple_query(sock, "use database info")
        assert any(kind == b"Z" for kind, _ in use_long), use_long
        alive = data_row_values(simple_query(sock, "SELECT 1 + 1"))
        assert alive == [[b"2"]], alive

        # FROM-less SELECT evaluates a single-row constant projection
        # (regression: any non-unnest projection raised "SQL syntax error").
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE FUNCTION dbl21(x INT) RETURNS INT LANGUAGE plpgsql AS "
            "$$ BEGIN RETURN x * 2; END $$"))
        udf_nop_rows = data_row_values(simple_query(sock, "SELECT dbl21(4)"))
        assert udf_nop_rows == [[b"8"]], udf_nop_rows
        const_rows = data_row_values(simple_query(sock, "SELECT 1 + 1"))
        assert const_rows == [[b"2"]], const_rows
        neg_rows = data_row_values(simple_query(sock, "SELECT -5"))
        assert neg_rows == [[b"-5"]], neg_rows
        mixed_rows = data_row_values(simple_query(
            sock, "SELECT 1 + 2 * 3, 'a' || 'b' AS cat"))
        assert mixed_rows == [[b"7", b"ab"]], mixed_rows
        cu_rows = data_row_values(simple_query(sock, "SELECT current_user"))
        assert cu_rows and cu_rows[0] and cu_rows[0][0], cu_rows
        # FROM-less UDF call with and without an alias.
        udf_rows = data_row_values(simple_query(sock, "SELECT dbl21(21)"))
        assert udf_rows == [[b"42"]], udf_rows
        udf_alias_rows = data_row_values(simple_query(sock, "SELECT dbl21(21) AS t"))
        assert udf_alias_rows == [[b"42"]], udf_alias_rows

        # EXECUTE FUNCTION triggers dispatch to the UDF runtime: a PL/pgSQL
        # trigger function must actually run (regression: the stored action
        # "name()" was executed as SQL and silently failed), string literals
        # in its INSERT body must land unquoted, and literal arguments must
        # reach the function.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE pl_trig_log (msg TEXT, n INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE FUNCTION pl_logit() RETURNS INT LANGUAGE plpgsql AS "
            "$$ BEGIN INSERT INTO pl_trig_log VALUES ('fired', 1); "
            "RETURN 1; END $$"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER view_trigger_base_pl INSTEAD OF INSERT ON "
            "writable_view FOR EACH ROW EXECUTE FUNCTION pl_logit()"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES (60, 'p1')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES (61, 'p2')"))
        pl_rows = data_row_values(simple_query(
            sock, "SELECT msg, n FROM pl_trig_log"))
        assert pl_rows == [[b"fired", b"1"], [b"fired", b"1"]], pl_rows

        # Literal arguments flow through EXECUTE FUNCTION call syntax.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE FUNCTION pl_tagv(x INT) RETURNS INT LANGUAGE plpgsql AS "
            "$$ BEGIN INSERT INTO pl_trig_log VALUES ('tagged', x); "
            "RETURN x; END $$"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER view_trigger_base_tag INSTEAD OF INSERT ON "
            "writable_view FOR EACH ROW EXECUTE FUNCTION pl_tagv(7)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES (62, 'p3')"))
        tag_rows = data_row_values(simple_query(
            sock, "SELECT msg, n FROM pl_trig_log WHERE msg = 'tagged'"))
        assert tag_rows == [[b"tagged", b"7"]], tag_rows

        # EXECUTE FUNCTION trigger bodies see NEW/OLD/TG_* variables:
        # INSERT exposes NEW (OLD null), UPDATE both images, DELETE OLD.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE tg_var_ev (op TEXT, tid INT, old_v TEXT, "
            "new_v TEXT, tname TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE FUNCTION tg_var_audit() RETURNS INT LANGUAGE plpgsql AS "
            "$$ BEGIN INSERT INTO tg_var_ev VALUES "
            "(tg_op, new.id, old.name, new.name, tg_relname); RETURN 0; END $$"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE FUNCTION tg_var_audit_del() RETURNS INT LANGUAGE plpgsql AS "
            "$$ BEGIN INSERT INTO tg_var_ev VALUES "
            "(tg_op, old.id, old.name, new.name, tg_relname); RETURN 0; END $$"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER tg_var_ins AFTER INSERT ON view_trigger_base "
            "FOR EACH ROW EXECUTE FUNCTION tg_var_audit()"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER tg_var_upd AFTER UPDATE ON view_trigger_base "
            "FOR EACH ROW EXECUTE FUNCTION tg_var_audit()"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER tg_var_del AFTER DELETE ON view_trigger_base "
            "FOR EACH ROW EXECUTE FUNCTION tg_var_audit_del()"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO view_trigger_base VALUES (70, 'i1')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UPDATE view_trigger_base SET name = 'i2' WHERE id = 70"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "DELETE FROM view_trigger_base WHERE id = 70"))
        tg_rows = data_row_values(simple_query(
            sock, "SELECT op, tid, old_v, new_v FROM tg_var_ev"))
        # The engine's tabular path renders NULL as the text "null" here.
        assert tg_rows == [
            [b"INSERT", b"70", b"null", b"i1"],
            [b"UPDATE", b"70", b"i1", b"i2"],
            [b"DELETE", b"70", b"i2", b"null"],
        ], tg_rows

        # Legacy SQL-action triggers keep routing through the SQL executor.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE pl_sql_log (n INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TRIGGER view_trigger_base_sql INSTEAD OF INSERT ON "
            "writable_view FOR EACH ROW INSERT INTO pl_sql_log VALUES (42)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO writable_view (id, name) VALUES (63, 'p4')"))
        sql_rows = data_row_values(simple_query(
            sock, "SELECT n FROM pl_sql_log"))
        assert sql_rows == [[b"42"]], sql_rows
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UPDATE writable_view SET name = 'bob' WHERE id > 0"))
        assert data_row_values(simple_query(
            sock, "SELECT name FROM view_trigger_base WHERE id = 10")) == [[b"bob"]]
        assert data_row_values(simple_query(
            sock, "SELECT name FROM view_trigger_base WHERE id = 11")) == [[b"bob"]]
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "DELETE FROM writable_view WHERE id > 0"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM view_trigger_base WHERE id = 10")) == []
        assert data_row_values(simple_query(
            sock, "SELECT id FROM view_trigger_base WHERE id = 11")) == []

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
        transaction_error_state_recovery(sock)
        prepared_transaction_error_boundaries(sock)
        error_messages = simple_query(sock, "SELECT * FROM protocol_missing_table")
        assert any(kind == b"E" for kind, _ in error_messages)
        assert error_messages[-1] == (b"Z", b"I")
        assert any(kind == b"C" for kind, _ in simple_query(sock, "SELECT id FROM t"))

        # Temporary tables are typed DDL, isolated by backend identity, and
        # survive ordinary statements in their owning session.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMP TABLE session_temp (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO session_temp VALUES (1)"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM session_temp")) == [[b"1"]]
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE temp_shadow (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO temp_shadow VALUES (99)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMPORARY TABLE temp_shadow (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO temp_shadow VALUES (7)"))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM temp_shadow")) == [[b"7"]]
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMP TABLE delete_rows (id INT) ON COMMIT DELETE ROWS"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO delete_rows VALUES (8)"))
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(
            sock, "SELECT id FROM delete_rows")) == []
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMP TABLE drop_rows (id INT) ON COMMIT DROP"))
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        dropped_temp = simple_query(sock, "SELECT id FROM drop_rows")
        assert any(kind == b"E" for kind, _ in dropped_temp), dropped_temp
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMP TABLE ctas_drop ON COMMIT DROP AS SELECT id FROM t"))
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        ctas_dropped = simple_query(sock, "SELECT id FROM ctas_drop")
        assert any(kind == b"E" for kind, _ in ctas_dropped), ctas_dropped
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TEMP TABLE rollback_temp (id INT)"))
        assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
        rolled_back_temp = simple_query(sock, "SELECT id FROM rollback_temp")
        assert any(kind == b"E" for kind, _ in rolled_back_temp), rolled_back_temp

        # A backend's transaction state must not leak through the shared
        # StorageEngine into another protocol connection.
        peer_sock = socket.socket()
        peer_sock.settimeout(SOCKET_TIMEOUT)
        peer_sock.connect(("127.0.0.1", port))
        startup(peer_sock, "alice", "info")
        assert any(kind == b"C" for kind, _ in simple_query(
            peer_sock, "CREATE TEMP TABLE peer_temp (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            peer_sock, "INSERT INTO peer_temp VALUES (2)"))
        assert data_row_values(simple_query(
            peer_sock, "SELECT id FROM peer_temp")) == [[b"2"]]
        peer_temp_in_owner = simple_query(sock, "SELECT id FROM peer_temp")
        assert any(kind == b"E" for kind, _ in peer_temp_in_owner), peer_temp_in_owner
        assert data_row_values(simple_query(sock, "SELECT id FROM session_temp")) == [[b"1"]]
        assert data_row_values(simple_query(peer_sock, "SELECT id FROM temp_shadow")) == [[b"99"]]
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

        # Transaction options and savepoint routing must be parsed structurally
        # rather than by fixed string offsets.
        assert simple_query(
            sock, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY")[-1] == (b"Z", b"T")
        assert data_row_values(simple_query(sock, "SELECT id FROM t")) == [[b"1"], [b"3"]]
        assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert simple_query(sock, "START TRANSACTION READ COMMITTED READ WRITE")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (20)"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "SAVEPOINT sp_tx"))
        assert any(kind == b"C" for kind, _ in simple_query(sock, "INSERT INTO t VALUES (21)"))
        assert simple_query(sock, "ROLLBACK TO SAVEPOINT sp_tx")[-1] == (b"Z", b"T")
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(sock, "SELECT id FROM t WHERE id >= 20")) == [[b"20"]]

        # Session settings must stay local to the connection.  pg_settings
        # exposes the effective value for the current backend.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "SET statement_timeout = 123"))
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "statement_timeout") == b"123"

        peer_sock.sendall(typed(b"X"))
        peer_sock.close()

        # Disconnecting a backend must abort its open transaction and discard
        # its context before the worker thread can be reused.
        leaked_sock = socket.socket()
        leaked_sock.settimeout(SOCKET_TIMEOUT)
        leaked_sock.connect(("127.0.0.1", port))
        startup(leaked_sock, "alice", "info")
        assert simple_query(leaked_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(leaked_sock, "INSERT INTO t VALUES (4)"))
        leaked_sock.sendall(typed(b"X"))
        leaked_sock.close()
        time.sleep(0.1)

        observer_sock = socket.socket()
        observer_sock.settimeout(SOCKET_TIMEOUT)
        observer_sock.connect(("127.0.0.1", port))
        startup(observer_sock, "alice", "info")
        peer_temp_after_disconnect = simple_query(observer_sock, "SELECT id FROM peer_temp")
        assert any(kind == b"E" for kind, _ in peer_temp_after_disconnect), peer_temp_after_disconnect
        assert simple_query(observer_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert data_row_values(simple_query(observer_sock, "SELECT id FROM t")) == [[b"1"], [b"3"], [b"20"]]
        assert simple_query(observer_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        role_sock = socket.socket()
        role_sock.settimeout(SOCKET_TIMEOUT)
        role_sock.connect(("127.0.0.1", port))
        startup(role_sock, "bob", "info", password="bObPass9!")
        # A newly opened backend gets the configured default, not another
        # backend's session override.
        assert setting_value(simple_query(role_sock, "SELECT * FROM pg_settings"),
                             "statement_timeout") == b"0"
        assert any(kind == b"C" for kind, _ in simple_query(
            role_sock, "SET statement_timeout = 456"))
        assert setting_value(simple_query(role_sock, "SELECT * FROM pg_settings"),
                             "statement_timeout") == b"456"
        assert setting_value(simple_query(observer_sock, "SELECT * FROM pg_settings"),
                             "statement_timeout") == b"0"
        assert any(kind == b"E" for kind, _ in simple_query(
            role_sock, "SET GLOBAL audit_level = 2"))
        assert any(kind == b"E" for kind, _ in simple_query(
            role_sock, "SET enable_seq_scan = off"))

        # Planner-affecting global changes invalidate the old EXPLAIN entry.
        first_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert not any(b"[plan cache hit]" in row[0]
                       for row in data_row_values(first_plan))
        second_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert any(b"[plan cache hit]" in row[0]
                   for row in data_row_values(second_plan))
        assert any(kind == b"C" for kind, _ in simple_query(
            observer_sock, "SET GLOBAL enable_seq_scan = off"))
        third_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert not any(b"[plan cache hit]" in row[0]
                       for row in data_row_values(third_plan))

        observer_sock.sendall(typed(b"X"))
        observer_sock.close()
        sock.sendall(typed(b"X"))
        sock.close()

        role_rows = data_row_values(simple_query(role_sock, "SELECT id FROM t"))
        assert role_rows == [[b"1"], [b"3"], [b"20"]], role_rows
        default_role_rows = data_row_values(simple_query(
            role_sock, "SELECT id FROM default_acl_protocol"))
        assert default_role_rows == [[b"7"]], default_role_rows
        assert any(kind == b"C" for kind, _ in simple_query(
            role_sock, "GRANT analyst TO carol WITH ADMIN OPTION"))
        assert any(kind == b"C" for kind, _ in simple_query(
            role_sock, "REVOKE ADMIN OPTION FOR analyst FROM carol"))
        assert any(kind == b"C" for kind, _ in simple_query(
            role_sock, "GRANT SELECT ON t TO carol WITH GRANT OPTION"))
        assert any(kind == b"C" for kind, _ in simple_query(
            role_sock, "REVOKE GRANT OPTION FOR SELECT ON t FROM carol"))
        denied_rows = simple_query(role_sock, "INSERT INTO t VALUES (99)")
        assert any(kind == b"E" for kind, _ in denied_rows)
        role_sock.sendall(typed(b"X"))
        role_sock.close()

        # In explicit plaintext mode the server must answer the PostgreSQL
        # SSLRequest with 'N' and continue with the normal startup packet.
        plain_sock = socket.socket()
        plain_sock.settimeout(SOCKET_TIMEOUT)
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
