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


DBMS_MAIN = os.path.abspath(os.environ.get(
    "DBMS_MAIN", os.path.join(os.path.dirname(__file__), "..", "dbms_main")))
SOCKET_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_TEST_TIMEOUT", "10"))
STARTUP_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_STARTUP_TIMEOUT", "15"))
SHUTDOWN_TIMEOUT = float(os.environ.get("DBMS_PROTOCOL_SHUTDOWN_TIMEOUT", "30"))


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


def startup(sock, user, database, password="secret", fragmented=False,
            application_name="dbms-protocol-test", protocol_version=196608,
            protocol_options=None, validate_dbms_status=True):
    protocol_options = protocol_options or {}
    params = (b"user\0" + user.encode() + b"\0database\0" + database.encode() +
              b"\0application_name\0" + application_name.encode() + b"\0")
    for name, value in protocol_options.items():
        params += name.encode() + b"\0" + value.encode() + b"\0"
    packet = frame(struct.pack("!I", protocol_version) + params + b"\0")
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
    negotiations = [body for kind, body in messages if kind == b"v"]
    expected_options = sorted(
        name.encode() for name in protocol_options if name.startswith("_pq_."))
    if protocol_version != 196608 or expected_options:
        assert len(negotiations) == 1, messages
        negotiation = negotiations[0]
        negotiated_version, option_count = struct.unpack("!II", negotiation[:8])
        assert negotiated_version == 196608
        options = negotiation[8:].split(b"\0")[:-1]
        assert option_count == len(options)
        assert sorted(options) == expected_options
    else:
        assert negotiations == [], messages
    statuses = {}
    for kind, body in messages:
        if kind != b"S":
            continue
        name, value, trailing = body.split(b"\0")
        assert trailing == b""
        statuses[name] = value
    if validate_dbms_status:
        assert statuses == {
            b"server_version": b"18.0 DBMS-C++ 0.2.0",
            b"server_encoding": b"UTF8",
            b"client_encoding": b"UTF8",
            b"application_name": application_name.encode(),
            b"DateStyle": b"ISO, MDY",
            b"IntervalStyle": b"postgres",
            b"is_superuser": b"on" if user == "alice" else b"off",
            b"session_authorization": user.encode(),
            b"default_transaction_read_only": b"off",
            b"in_hot_standby": b"off",
            b"integer_datetimes": b"on",
            b"standard_conforming_strings": b"on",
            b"TimeZone": b"UTC",
        }, statuses
    backend_key = next(body for kind, body in messages if kind == b"K")
    return struct.unpack("!II", backend_key)


def startup_reference(sock, user, database, password="secret"):
    """Use the wire reader against PostgreSQL without DBMS-only status claims."""
    return startup(sock, user, database, password=password,
                   validate_dbms_status=False)


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


def notification_values(messages):
    values = []
    for kind, body in messages:
        if kind != b"A":
            continue
        sender_pid = struct.unpack("!I", body[:4])[0]
        channel_end = body.index(b"\0", 4)
        payload_end = body.index(b"\0", channel_end + 1)
        values.append((sender_pid, body[4:channel_end],
                       body[channel_end + 1:payload_end]))
    return values


def diagnostic_fields(body):
    fields = {}
    offset = 0
    while body[offset] != 0:
        field = body[offset:offset + 1]
        offset += 1
        end = body.index(b"\0", offset)
        fields[field] = body[offset:end]
        offset = end + 1
    return fields


def wait_for_disconnect(sock, timeout=2.0):
    deadline = time.time() + timeout
    sock.settimeout(0.1)
    while time.time() < deadline:
        try:
            if sock.recv(1) == b"":
                return True
        except (ConnectionResetError, BrokenPipeError):
            return True
        except socket.timeout:
            pass
    return False


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
    assert kind == b"1" and body == b"", (kind, body)

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
    assert kind == b"1" and body == b"", (kind, body)

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


def extended_query_boolean_text_parameter(sock):
    statement = b"bool_text"
    portal = b"bool_text_portal"
    sql = b"SELECT $1::boolean"
    parse = (statement + b"\0" + sql + b"\0" + struct.pack("!H", 1) +
             struct.pack("!I", 16))
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"1" and body == b"", (kind, body)

    # "ye" is an unambiguous PostgreSQL boolean-input abbreviation.
    raw = b"ye"
    bind = (portal + b"\0" + statement + b"\0" + struct.pack("!H", 0) +
            struct.pack("!H", 1) + struct.pack("!i", len(raw)) + raw +
            struct.pack("!H", 0))
    sock.sendall(typed(b"B", bind))
    kind, body = read_message(sock)
    assert kind == b"2" and body == b"", (kind, body)
    sock.sendall(typed(b"E", portal + b"\0" + struct.pack("!I", 0)) +
                 typed(b"S"))
    messages = read_until_ready(sock)
    fields = row_description_fields(messages)
    assert len(fields) == 1 and fields[0][3] == 16, fields
    assert data_row_values(messages) == [[b"t"]], messages

    sock.sendall(typed(b"C", b"P" + portal + b"\0") +
                 typed(b"C", b"S" + statement + b"\0") + typed(b"S"))
    messages = read_until_ready(sock)
    assert sum(kind == b"3" for kind, _ in messages) == 2, messages

def extended_query_binary_int_parameter(sock, sql, value):
    parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 1) + struct.pack("!I", 23)
    sock.sendall(typed(b"P", parse))
    kind, body = read_message(sock)
    assert kind == b"1" and body == b"", (kind, body)

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
    assert kind == b"1" and body == b"", (kind, body)

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
    positive_infinity = struct.pack("!q", (1 << 63) - 1)
    negative_infinity = struct.pack("!q", -(1 << 63))
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
        sock, "timestamp_pos_inf",
        "SELECT ts FROM protocol_infinity_pos WHERE ts = $1", 1114,
        positive_infinity, positive_infinity)
    extended_query_binary_parameter(
        sock, "timestamp_neg_inf",
        "SELECT ts FROM protocol_infinity_neg WHERE ts = $1", 1114,
        negative_infinity, negative_infinity)
    extended_query_binary_parameter(
        sock, "timestamptz_pos_inf",
        "SELECT tz FROM protocol_infinity_pos WHERE tz = $1", 1184,
        positive_infinity, positive_infinity)
    extended_query_binary_parameter(
        sock, "timestamptz_neg_inf",
        "SELECT tz FROM protocol_infinity_neg WHERE tz = $1", 1184,
        negative_infinity, negative_infinity)
    extended_query_binary_parameter(
        sock, "timestamp_filter",
        "SELECT ts FROM protocol_timestamp_filter WHERE ts = $1", 1114,
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
    assert kind == b"1" and body == b"", (kind, body)

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
    # Distinguish a genuinely missing local transaction from an active
    # transaction whose backend-local state cannot safely enter 2PC.
    missing_local = simple_query(sock, "PREPARE TRANSACTION 'no_local_txn'")
    missing_error = next(body for kind, body in missing_local if kind == b"E")
    assert b"C25P01\0" in missing_error, missing_local
    assert missing_local[-1] == (b"Z", b"I"), missing_local

    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "INSERT INTO t VALUES (440)"))
    prepared_ok = simple_query(sock, "PREPARE TRANSACTION 'protocol_2pc_ok'")
    assert any(kind == b"C" and body == b"PREPARE TRANSACTION\0"
               for kind, body in prepared_ok), prepared_ok
    assert prepared_ok[-1] == (b"Z", b"I"), prepared_ok
    rollback_prepared = simple_query(
        sock, "ROLLBACK PREPARED 'protocol_2pc_ok'")
    assert any(kind == b"C" and body == b"ROLLBACK PREPARED\0"
               for kind, body in rollback_prepared), rollback_prepared
    assert data_row_values(simple_query(
        sock, "SELECT id FROM t WHERE id = 440")) == []

    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "ALTER TABLE t ADD COLUMN prepared_note TEXT"))
    ddl_prepared = simple_query(sock, "PREPARE TRANSACTION 'ddl_protocol_xid'")
    ddl_error = next(body for kind, body in ddl_prepared if kind == b"E")
    assert b"C0A000\0" in ddl_error, ddl_prepared
    assert ddl_prepared[-1] == (b"Z", b"E"), ddl_prepared
    assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
    restored_fields = row_description_fields(simple_query(sock, "SELECT * FROM t"))
    assert [field[0] for field in restored_fields] == [b"id"], restored_fields

    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "CREATE TABLE deferred_parent (id INT PRIMARY KEY)"))
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "CREATE TABLE deferred_child (id INT PRIMARY KEY, pid INT, "
              "CONSTRAINT deferred_child_fk FOREIGN KEY (pid) "
              "REFERENCES deferred_parent(id) DEFERRABLE INITIALLY DEFERRED)"))
    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "INSERT INTO deferred_child VALUES (1, 999)"))
    fk_prepare = simple_query(
        sock, "PREPARE TRANSACTION 'deferred_fk_protocol'")
    fk_error = next(body for kind, body in fk_prepare if kind == b"E")
    assert b"C23503\0" in fk_error, fk_prepare
    assert fk_prepare[-1] == (b"Z", b"E"), fk_prepare
    assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
    assert data_row_values(simple_query(
        sock, "SELECT id FROM deferred_child")) == []

    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "CREATE TABLE deferred_check (id INT PRIMARY KEY, value INT, "
              "CONSTRAINT deferred_positive CHECK (value > 0) "
              "DEFERRABLE INITIALLY DEFERRED)"))
    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "INSERT INTO deferred_check VALUES (1, 0)"))
    check_commit = simple_query(sock, "COMMIT")
    check_error = next(body for kind, body in check_commit if kind == b"E")
    assert b"C23514\0" in check_error, check_commit
    assert check_commit[-1] == (b"Z", b"E"), check_commit
    assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
    assert data_row_values(simple_query(
        sock, "SELECT id FROM deferred_check")) == []

    assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
    assert any(kind == b"C" for kind, _ in simple_query(
        sock, "NOTIFY prepared_notify, 'must_not_escape'"))
    notify_prepare = simple_query(
        sock, "PREPARE TRANSACTION 'prepared_notify_protocol'")
    notify_error = next(body for kind, body in notify_prepare if kind == b"E")
    assert b"C0A000\0" in notify_error, notify_prepare
    assert notify_prepare[-1] == (b"Z", b"E"), notify_prepare
    assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")

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
        with open(os.path.join(work_dir, "dbms.conf"), "w", encoding="utf-8") as config:
            config.write("max_connections=1\nmax_notify_queue_pages=1\n")

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        process = subprocess.Popen(
            [DBMS_MAIN, "--server", str(port), "--insecure"],
            cwd=work_dir,
            # Parts of this regression exercise extended-mode project
            # commands (SET GLOBAL plan invalidation, DIV-11).
            env=dict(os.environ, DBMS_COMPATIBILITY_MODE="extended"),
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

        # max_connections controls the live accept path at startup.  ALTER
        # SYSTEM/extended SET GLOBAL only persist a pending restart value.
        limited_sock = socket.socket()
        limited_sock.connect(("127.0.0.1", port))
        assert wait_for_disconnect(limited_sock), "startup max_connections was ignored"
        limited_sock.close()

        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "SET GLOBAL max_connections = 2"))
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "max_connections") == b"1"
        with open(os.path.join(work_dir, "dbms.conf"), encoding="utf-8") as config:
            assert "max_connections=2\n" in config.read()
        reload_messages = simple_query(sock, "SELECT pg_reload_conf()")
        assert data_row_values(reload_messages) == [[b"t"]], reload_messages
        reload_fields = row_description_fields(reload_messages)
        assert len(reload_fields) == 1, reload_fields
        assert reload_fields[0][0] == b"pg_reload_conf", reload_fields
        assert reload_fields[0][3] == 16, reload_fields
        assert (b"C", b"SELECT 1\0") in reload_messages, reload_messages
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "max_connections") == b"1"
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "SET GLOBAL max_connections = 64"))

        # A restart installs the persisted value.
        sock.sendall(typed(b"X"))
        sock.close()
        process.terminate()
        # Graceful SIGTERM includes durable catalog/WAL/cache writeback.  A
        # populated protocol-test cluster can legitimately need more than
        # five seconds on a busy or slower filesystem; keep the bound finite
        # and independently configurable instead of racing the flush path.
        process.wait(timeout=SHUTDOWN_TIMEOUT)
        process = subprocess.Popen(
            [DBMS_MAIN, "--server", str(port), "--insecure"],
            cwd=work_dir,
            env=dict(os.environ, DBMS_COMPATIBILITY_MODE="extended"),
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
        startup(sock, "alice", "info", protocol_version=196610,
                protocol_options={"_pq_.unsupported_test": "1"})
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "max_connections") == b"64"
        malformed_startup_sock = socket.socket()
        malformed_startup_sock.settimeout(SOCKET_TIMEOUT)
        malformed_startup_sock.connect(("127.0.0.1", port))
        malformed_startup_sock.sendall(frame(
            struct.pack("!I", 196608) + b"user\0alice\0"))
        kind, body = read_message(malformed_startup_sock)
        assert kind == b"E" and b"C08P01\0" in body, (kind, body)
        assert wait_for_disconnect(malformed_startup_sock)
        malformed_startup_sock.close()
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "SET GLOBAL max_notify_queue_pages = 2"))
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "max_notify_queue_pages") == b"1"
        reload_messages = simple_query(sock, "SELECT pg_reload_conf()")
        assert data_row_values(reload_messages) == [[b"t"]], reload_messages
        assert setting_value(simple_query(sock, "SELECT * FROM pg_settings"),
                             "max_notify_queue_pages") == b"1"

        # Executor NOTICE/WARNING lines are asynchronous NoticeResponse
        # frames with the standard diagnostic fields.  They must not become
        # result rows or replace the command tag.
        notice_messages = simple_query(
            sock, "DROP TABLE IF EXISTS protocol_notice_missing")
        notice_index = next(i for i, message in enumerate(notice_messages)
                            if message[0] == b"N")
        command_index = next(i for i, message in enumerate(notice_messages)
                             if message[0] == b"C")
        fields = diagnostic_fields(notice_messages[notice_index][1])
        assert fields == {
            b"S": b"NOTICE", b"V": b"NOTICE", b"C": b"00000",
            b"M": b'table "protocol_notice_missing" does not exist, skipping'
        }, fields
        assert notice_index < command_index, notice_messages
        assert not any(kind in (b"T", b"D") for kind, _ in notice_messages), \
            notice_messages

        # A Simple Query message may carry multiple statements.  Split only
        # on top-level semicolons, return one result per statement and one
        # ReadyForQuery for the whole message.
        multi_messages = simple_query(
            sock, "SELECT ';' AS semicolon /* ; nested /* ; */ */; "
                  "SELECT 2 AS second_value -- ; in comment\n; "
                  "SELECT 3 AS third_value")
        assert data_row_values(multi_messages) == [
            [b";"], [b"2"], [b"3"]
        ], multi_messages
        assert sum(kind == b"T" for kind, _ in multi_messages) == 3, multi_messages
        assert sum(kind == b"C" for kind, _ in multi_messages) == 3, multi_messages
        assert sum(kind == b"Z" for kind, _ in multi_messages) == 1, multi_messages

        # In the absence of explicit transaction control, the statements in
        # one Q message are one implicit transaction.  Stop at the first
        # error and roll back earlier writes; never execute later statements.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_batch_atomic (id INT PRIMARY KEY)"))
        batch_error = simple_query(
            sock, "INSERT INTO protocol_batch_atomic VALUES (80); "
                  "INSERT INTO protocol_missing_table VALUES (1); "
                  "INSERT INTO protocol_batch_atomic VALUES (81)")
        assert sum(kind == b"C" for kind, _ in batch_error) == 1, batch_error
        assert sum(kind == b"E" for kind, _ in batch_error) == 1, batch_error
        assert batch_error[-1] == (b"Z", b"I"), batch_error
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_batch_atomic")) == []

        explicit_batch = simple_query(
            sock, "BEGIN; INSERT INTO protocol_batch_atomic VALUES (82); "
                  "SELECT id FROM protocol_batch_atomic WHERE id = 82")
        assert explicit_batch[-1] == (b"Z", b"T"), explicit_batch
        assert data_row_values(explicit_batch) == [[b"82"]], explicit_batch
        assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_batch_atomic WHERE id = 82")) == []

        # Control messages have fixed bodies.  A NUL-terminated Query cannot
        # hide trailing SQL, and malformed Flush/Sync frames must produce a
        # protocol-violation error without desynchronizing the connection.
        sock.sendall(typed(b"Q", b"SELECT 11\0SELECT 12\0"))
        malformed_query = read_until_ready(sock)
        assert not any(kind == b"D" for kind, _ in malformed_query), malformed_query
        assert b"C08P01\0" in next(
            body for kind, body in malformed_query if kind == b"E")
        assert malformed_query[-1] == (b"Z", b"I"), malformed_query

        sock.sendall(typed(b"H", b"garbage") + typed(b"S"))
        malformed_flush = read_until_ready(sock)
        assert b"C08P01\0" in next(
            body for kind, body in malformed_flush if kind == b"E")
        sock.sendall(typed(b"S", b"garbage"))
        malformed_sync = read_until_ready(sock)
        assert b"C08P01\0" in next(
            body for kind, body in malformed_sync if kind == b"E")
        assert data_row_values(simple_query(sock, "SELECT 13")) == [[b"13"]]

        # Named statements and portals cannot be silently replaced.  A
        # duplicate Parse/Bind enters extended-query recovery, keeps the
        # original object intact, and reports PostgreSQL's dedicated codes.
        named_parse = (b"duplicate_stmt\0SELECT 21\0" +
                       struct.pack("!H", 0))
        sock.sendall(typed(b"P", named_parse))
        assert read_message(sock) == (b"1", b"")
        duplicate_parse = (b"duplicate_stmt\0SELECT 22\0" +
                           struct.pack("!H", 0))
        sock.sendall(typed(b"P", duplicate_parse))
        kind, body = read_message(sock)
        assert kind == b"E" and b"C42P05\0" in body, (kind, body)
        sock.sendall(typed(b"S"))
        assert read_until_ready(sock)[-1] == (b"Z", b"I")

        named_bind = (b"duplicate_portal\0duplicate_stmt\0" +
                      struct.pack("!H", 0) + struct.pack("!H", 0) +
                      struct.pack("!H", 0))
        sock.sendall(typed(b"B", named_bind))
        assert read_message(sock) == (b"2", b"")
        sock.sendall(typed(b"B", named_bind))
        kind, body = read_message(sock)
        assert kind == b"E" and b"C42P03\0" in body, (kind, body)
        sock.sendall(typed(b"S"))
        assert read_until_ready(sock)[-1] == (b"Z", b"I")
        sock.sendall(typed(
            b"E", b"duplicate_portal\0" + struct.pack("!I", 0)) +
            typed(b"S"))
        duplicate_object_result = read_until_ready(sock)
        assert data_row_values(duplicate_object_result) == [[b"21"]], \
            duplicate_object_result

        # LISTEN/NOTIFY is backend-local, transactional, and transported as
        # protocol NotificationResponse rather than text prepended to a query.
        notify_sock = socket.socket()
        notify_sock.settimeout(SOCKET_TIMEOUT)
        notify_sock.connect(("127.0.0.1", port))
        notify_pid, _ = startup(notify_sock, "alice", "info")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "LISTEN wire_channel"))

        assert simple_query(notify_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "NOTIFY wire_channel, 'rolled back'"))
        assert simple_query(notify_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert notification_values(simple_query(sock, "SELECT 1")) == []

        assert simple_query(notify_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "NOTIFY wire_channel, 'before savepoint'"))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "SAVEPOINT notify_sp"))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "NOTIFY wire_channel, 'after savepoint'"))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "ROLLBACK TO SAVEPOINT notify_sp"))
        assert simple_query(notify_sock, "COMMIT")[-1] == (b"Z", b"I")
        idle_notification = read_message(sock)
        assert notification_values([idle_notification]) == [
            (notify_pid, b"wire_channel", b"before savepoint")]

        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UNLISTEN wire_channel"))
        assert simple_query(sock, "ROLLBACK")[-1] == (b"Z", b"I")
        committed_notify = simple_query(
            notify_sock, "NOTIFY wire_channel, 'unlisten rolled back'")
        assert any(kind == b"C" for kind, _ in committed_notify)
        assert notification_values(simple_query(sock, "SELECT 1")) == [
            (notify_pid, b"wire_channel", b"unlisten rolled back")]

        # A listener inside a transaction retains notifications until it
        # reaches a transaction boundary.  In particular, the server's idle
        # socket poll and statements inside that transaction must not emit A.
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "NOTIFY wire_channel, 'receiver transaction boundary'"))
        in_listener_transaction = simple_query(sock, "SELECT 1")
        assert notification_values(in_listener_transaction) == []
        listener_commit = simple_query(sock, "COMMIT")
        assert notification_values(listener_commit) == [
            (notify_pid, b"wire_channel", b"receiver transaction boundary")]
        assert listener_commit[-1] == (b"Z", b"I")

        empty_queue_usage = simple_query(
            notify_sock, "SELECT pg_notification_queue_usage()")
        assert float(data_row_values(empty_queue_usage)[0][0]) == 0.0
        assert row_description_fields(empty_queue_usage)[0][3:5] == (701, 8)

        # With a one-page startup queue, one near-maximum payload occupies
        # most of the queue while the listener is in a transaction.  A second
        # transaction must fail before commit with PostgreSQL's 54000, then
        # the first logical entry is released after the receiver consumes it.
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "CREATE TABLE notify_queue_atomic (id INT)"))
        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        queue_payload = "q" * 7900
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "NOTIFY wire_channel, '" + queue_payload + "'"))
        occupied_queue_usage = float(data_row_values(simple_query(
            notify_sock,
            "SELECT pg_notification_queue_usage()"))[0][0])
        assert 0.9 < occupied_queue_usage < 1.0
        assert simple_query(notify_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "INSERT INTO notify_queue_atomic VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "NOTIFY wire_channel, '" + ("r" * 7900) + "'"))
        full_queue = simple_query(notify_sock, "COMMIT")
        assert any(kind == b"E" and b"C54000\x00" in body
                   for kind, body in full_queue), full_queue
        assert simple_query(notify_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(
            notify_sock, "SELECT id FROM notify_queue_atomic")) == []
        queue_drain = simple_query(sock, "COMMIT")
        assert notification_values(queue_drain) == [
            (notify_pid, b"wire_channel", queue_payload.encode())]
        assert float(data_row_values(simple_query(
            notify_sock,
            "SELECT pg_notification_queue_usage()"))[0][0]) == 0.0

        # Two concurrent sessions authenticated as the same role must each
        # retain their own queue; consuming one must not consume the other.
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "LISTEN shared_role_channel"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "LISTEN shared_role_channel"))
        self_notify = simple_query(
            notify_sock, "NOTIFY shared_role_channel, 'both backends'")
        assert notification_values(self_notify) == [
            (notify_pid, b"shared_role_channel", b"both backends")]
        assert notification_values(simple_query(sock, "SELECT 1")) == [
            (notify_pid, b"shared_role_channel", b"both backends")]

        assert simple_query(sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "UNLISTEN wire_channel"))
        assert simple_query(sock, "COMMIT")[-1] == (b"Z", b"I")
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock, "NOTIFY wire_channel, 'after unlisten commit'"))
        assert notification_values(simple_query(sock, "SELECT 1")) == []

        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE DATABASE notify_other"))
        other_db_sock = socket.socket()
        other_db_sock.settimeout(SOCKET_TIMEOUT)
        other_db_sock.connect(("127.0.0.1", port))
        startup(other_db_sock, "alice", "notify_other")
        assert any(kind == b"C" for kind, _ in simple_query(
            other_db_sock,
            "NOTIFY shared_role_channel, 'other database'"))
        assert notification_values(simple_query(sock, "SELECT 1")) == []
        other_db_sock.sendall(typed(b"X"))
        other_db_sock.close()

        # Channel identifiers are parsed (including quoted case, punctuation,
        # and doubled quotes) and payload string literals are decoded. Invalid
        # trailing tokens and the 8000-byte payload boundary fail closed.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, 'LISTEN "MiXed,Channel"'))
        quoted_notify = simple_query(
            notify_sock, 'NOTIFY "MiXed,Channel", \'It\'\'s Mixed\'')
        assert any(kind == b"C" for kind, _ in quoted_notify)
        assert notification_values([read_message(sock)]) == [
            (notify_pid, b"MiXed,Channel", b"It's Mixed")]

        long_statement_channel = "n" * 64
        truncated_statement_channel = b"n" * 63
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "LISTEN " + long_statement_channel))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "NOTIFY " + long_statement_channel + ", 'identifier clipped'"))
        assert notification_values([read_message(sock)]) == [
            (notify_pid, truncated_statement_channel, b"identifier clipped")]

        for invalid_notification_sql in (
                "LISTEN two words",
                "NOTIFY wire_channel, payload",
                "NOTIFY wire_channel, 'payload' trailing"):
            invalid_notification = simple_query(
                notify_sock, invalid_notification_sql)
            assert any(kind == b"E" and b"C42601\x00" in body
                       for kind, body in invalid_notification), \
                invalid_notification

        oversized_notify = simple_query(
            notify_sock,
            "NOTIFY wire_channel, '" + ("x" * 8000) + "'")
        assert any(kind == b"E" and b"C22023\x00" in body
                   for kind, body in oversized_notify), oversized_notify
        assert notification_values(simple_query(sock, "SELECT 1")) == []

        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "LISTEN function_channel"))
        function_notify = simple_query(
            notify_sock,
            "SELECT pg_notify('function_' || 'channel', "
            "'pay' || 'load')")
        assert data_row_values(function_notify) == [[b""]]
        function_fields = row_description_fields(function_notify)
        assert function_fields[0][0] == b"pg_notify"
        assert function_fields[0][3:5] == (2278, 4)
        assert notification_values([read_message(sock)]) == [
            (notify_pid, b"function_channel", b"payload")]

        null_payload_notify = simple_query(
            notify_sock, "SELECT pg_notify('function_channel', NULL)")
        assert data_row_values(null_payload_notify) == [[b""]]
        assert notification_values([read_message(sock)]) == [
            (notify_pid, b"function_channel", b"")]

        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "CREATE TABLE notify_payloads (payload TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            notify_sock,
            "INSERT INTO notify_payloads VALUES ('row one'), ('row two')"))
        row_function_notify = simple_query(
            notify_sock,
            "SELECT pg_notify('function_channel', payload) "
            "FROM notify_payloads ORDER BY payload")
        assert data_row_values(row_function_notify) == [[b""], [b""]]
        assert row_description_fields(row_function_notify)[0][3:5] == (2278, 4)
        assert [notification_values([read_message(sock)]),
                notification_values([read_message(sock)])] == [
            [(notify_pid, b"function_channel", b"row one")],
            [(notify_pid, b"function_channel", b"row two")]]

        # A top-level SELECT is still a transaction for notification side
        # effects: a later expression error and an explicit rollback must
        # discard a pg_notify call already evaluated in that statement/txn.
        failed_function_notify = simple_query(
            notify_sock,
            "SELECT pg_notify('function_channel', 'must not escape'), 1/0")
        assert any(kind == b"E" and b"C22012\x00" in body
                   for kind, body in failed_function_notify), \
            failed_function_notify
        assert notification_values(simple_query(sock, "SELECT 1")) == []

        assert simple_query(notify_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert data_row_values(simple_query(
            notify_sock,
            "SELECT pg_notify('function_channel', 'rolled back function')")) \
            == [[b""]]
        assert simple_query(notify_sock, "ROLLBACK")[-1] == (b"Z", b"I")
        assert notification_values(simple_query(sock, "SELECT 1")) == []

        for invalid_function_notify_sql, expected_state in (
                ("SELECT pg_notify(NULL, 'payload')", b"C22023\x00"),
                ("SELECT pg_notify('', 'payload')", b"C22023\x00"),
                ("SELECT pg_notify('" + ("c" * 64) + "', 'payload')",
                 b"C22023\x00"),
                ("SELECT pg_notify('function_channel')", b"C42883\x00")):
            invalid_function_notify = simple_query(
                notify_sock, invalid_function_notify_sql)
            assert any(kind == b"E" and expected_state in body
                       for kind, body in invalid_function_notify), \
                invalid_function_notify

        # pg_listening_channels() is a backend-local SRF and exposes only the
        # committed subscription set, in deterministic channel order.
        inspect_sock = socket.socket()
        inspect_sock.settimeout(SOCKET_TIMEOUT)
        inspect_sock.connect(("127.0.0.1", port))
        startup(inspect_sock, "alice", "info")
        empty_channels = simple_query(
            inspect_sock, "SELECT pg_listening_channels()")
        assert data_row_values(empty_channels) == []
        assert row_description_fields(empty_channels)[0][0] == \
            b"pg_listening_channels"
        assert row_description_fields(empty_channels)[0][3:5] == (25, -1)
        assert any(kind == b"C" for kind, _ in simple_query(
            inspect_sock, "LISTEN inspect_beta"))
        assert any(kind == b"C" for kind, _ in simple_query(
            inspect_sock, "LISTEN inspect_alpha"))
        committed_channels = simple_query(
            inspect_sock,
            "SELECT pg_listening_channels(), 7 AS marker")
        assert data_row_values(committed_channels) == [
            [b"inspect_alpha", b"7"], [b"inspect_beta", b"7"]]

        assert simple_query(inspect_sock, "BEGIN")[-1] == (b"Z", b"T")
        assert any(kind == b"C" for kind, _ in simple_query(
            inspect_sock, "UNLISTEN inspect_alpha"))
        assert any(kind == b"C" for kind, _ in simple_query(
            inspect_sock, "LISTEN inspect_gamma"))
        assert data_row_values(simple_query(
            inspect_sock, "SELECT pg_listening_channels()")) == [
                [b"inspect_alpha"], [b"inspect_beta"]]
        assert simple_query(inspect_sock, "COMMIT")[-1] == (b"Z", b"I")
        assert data_row_values(simple_query(
            inspect_sock, "SELECT pg_listening_channels()")) == [
                [b"inspect_beta"], [b"inspect_gamma"]]
        assert any(kind == b"C" for kind, _ in simple_query(
            inspect_sock, "UNLISTEN *"))
        assert data_row_values(simple_query(
            inspect_sock, "SELECT pg_listening_channels()")) == []
        inspect_sock.sendall(typed(b"X"))
        inspect_sock.close()

        notify_sock.sendall(typed(b"X"))
        notify_sock.close()

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
        # PG sends a -1 length (client NULL) for a stored NULL id; the
        # planner now propagates the stored null bit to the wire.
        assert data_row_values(simple_query(
            sock, "SELECT id FROM dml_ast")) == [[b"7"], [b"2"], [None]]
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
        returning_empty = simple_query(
            sock, "INSERT INTO dml_ast VALUES (12, '') "
            "RETURNING name, name || '-x' AS tagged")
        assert data_row_values(returning_empty) == [[b"", b"-x"]], returning_empty
        returning_empty_update = simple_query(
            sock, "UPDATE dml_ast SET name = '' WHERE id = 12 "
            "RETURNING name, name || '-u' AS tagged")
        assert data_row_values(returning_empty_update) == [
            [b"", b"-u"]], returning_empty_update
        returning_empty_delete = simple_query(
            sock, "DELETE FROM dml_ast WHERE id = 12 RETURNING name")
        assert data_row_values(returning_empty_delete) == [
            [b""]], returning_empty_delete
        returning_null_text = simple_query(
            sock, "INSERT INTO dml_ast VALUES (13, 'NULL') "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_null_text) == [
            [b"NULL", b"f"]], returning_null_text
        returning_sql_null = simple_query(
            sock, "INSERT INTO dml_ast VALUES (14, NULL) "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_sql_null) == [
            [None, b"t"]], returning_sql_null
        returning_null_text_update = simple_query(
            sock, "UPDATE dml_ast SET name = upper('null') WHERE id = 14 "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_null_text_update) == [
            [b"NULL", b"f"]], returning_null_text_update
        returning_sql_null_update = simple_query(
            sock, "UPDATE dml_ast SET name = NULL WHERE id = 13 "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_sql_null_update) == [
            [None, b"t"]], returning_sql_null_update
        selected_null_text = simple_query(
            sock, "SELECT id FROM dml_ast WHERE name = 'NULL'")
        assert data_row_values(selected_null_text) == [
            [b"14"]], selected_null_text
        returning_null_text_delete = simple_query(
            sock, "DELETE FROM dml_ast WHERE id = 14 "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_null_text_delete) == [
            [b"NULL", b"f"]], returning_null_text_delete
        returning_sql_null_delete = simple_query(
            sock, "DELETE FROM dml_ast WHERE id = 13 "
            "RETURNING name, name IS NULL")
        assert data_row_values(returning_sql_null_delete) == [
            [None, b"t"]], returning_sql_null_delete
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
        correlated_not_exists_rows = data_row_values(simple_query(
            sock, "SELECT id FROM sub_outer "
            "WHERE NOT EXISTS (SELECT 1 FROM sub_inner "
            "WHERE sub_inner.id = sub_outer.id)"))
        assert correlated_not_exists_rows == [[b"1"], [b"4"]], correlated_not_exists_rows
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
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE sub_nullable (id INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO sub_nullable VALUES (1), (NULL)"))
        empty_not_in_nullable = data_row_values(simple_query(
            sock, "SELECT id FROM sub_nullable "
            "WHERE id NOT IN (SELECT id FROM sub_inner WHERE enabled = 9)"))
        assert empty_not_in_nullable == [[b"1"], [None]], empty_not_in_nullable
        empty_all_nullable = data_row_values(simple_query(
            sock, "SELECT id FROM sub_nullable "
            "WHERE id > ALL (SELECT id FROM sub_inner WHERE enabled = 9)"))
        assert empty_all_nullable == [[b"1"], [None]], empty_all_nullable
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
            sock, "CREATE TABLE protocol_infinity_pos "
            "(ts TIMESTAMP, tz TIMESTAMPTZ)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO protocol_infinity_pos "
            "VALUES ('infinity', 'infinity')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_infinity_neg "
            "(ts TIMESTAMP, tz TIMESTAMPTZ)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO protocol_infinity_neg "
            "VALUES ('-infinity', '-infinity')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE protocol_timestamp_filter "
            "(id INT, ts TIMESTAMP, label TEXT)"))
        for identifier, timestamp, label in (
                (1, "2025-01-01 00:00:00", "early value"),
                (2, "2026-08-07 12:34:56", "target (value)"),
                (3, "2027-12-31 23:59:59", "late value"),
                (4, "2028-06-30 12:00:00", "escaped ''quote'' value"),
                (5, "2029-07-01 12:00:00", "O''Brien")):
            assert any(kind == b"C" for kind, _ in simple_query(
                sock, "INSERT INTO protocol_timestamp_filter VALUES "
                "(%d, '%s', '%s')" % (identifier, timestamp, label)))
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_timestamp_filter "
            "WHERE label = 'early value'")) == [[b"1"]]
        label_messages = simple_query(
            sock, "SELECT id FROM protocol_timestamp_filter "
            "WHERE label = 'target (value)'")
        assert data_row_values(label_messages) == [[b"2"]], label_messages
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_timestamp_filter "
            "WHERE label = 'escaped ''quote'' value'")) == [[b"4"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_timestamp_filter "
            "WHERE label LIKE 'escaped ''quote''%'")) == [[b"4"]]
        assert data_row_values(simple_query(
            sock, "SELECT id FROM protocol_timestamp_filter "
            "WHERE label IN ('O''Brien')")) == [[b"5"]]
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
        complex_set_messages = simple_query(
            sock, "SELECT id FROM set_left UNION SELECT count(*) FROM set_right")
        complex_set_rows = data_row_values(complex_set_messages)
        assert complex_set_rows == [[b"1"], [b"2"], [b"3"], [b"4"]], (
            complex_set_rows, complex_set_messages)

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

        # UPDATE with NOT IN predicates and table aliases (PG shapes):
        # "UPDATE t SET ... WHERE c NOT IN (...)" previously failed with a
        # syntax error ("unexpected token: not"); "UPDATE t x SET ..." was
        # rejected ("UPDATE requires SET").
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE upd_t (id INT PRIMARY KEY, v TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO upd_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')"))
        simple_query(sock, "UPDATE upd_t SET v = 'q' WHERE id NOT IN (1,2,3)")
        uq1 = data_row_values(simple_query(sock, "SELECT v FROM upd_t WHERE id = 4"))
        assert uq1 == [[b"q"]], uq1
        simple_query(sock, "UPDATE upd_t x SET v = 'r' WHERE x.id = 1")
        uq2 = data_row_values(simple_query(sock, "SELECT v FROM upd_t WHERE id = 1"))
        assert uq2 == [[b"r"]], uq2
        simple_query(sock, "UPDATE upd_t AS u SET v = 's' WHERE u.id = 2")
        uq3 = data_row_values(simple_query(sock, "SELECT v FROM upd_t WHERE id = 2"))
        assert uq3 == [[b"s"]], uq3
        simple_query(sock, "DELETE FROM upd_t WHERE id NOT IN (1,2,3)")
        uq4 = data_row_values(simple_query(sock, "SELECT COUNT(*) FROM upd_t"))
        assert uq4 == [[b"3"]], uq4

        # Single-predicate NOT LIKE / BETWEEN / NOT BETWEEN (no AND): the
        # condition previously never reached the engine (0 rows / all rows).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE pred_t (id INT PRIMARY KEY, name TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO pred_t VALUES (1,'ann'),(2,'bob'),(3,'cat')"))
        nl1 = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name NOT LIKE 'a%' ORDER BY id"))
        assert nl1 == [[b"2"], [b"3"]], nl1
        bt1 = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE id BETWEEN 1 AND 2 ORDER BY id"))
        assert bt1 == [[b"1"], [b"2"]], bt1
        nb1 = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE id NOT BETWEEN 1 AND 2"))
        assert nb1 == [[b"3"]], nb1
        tb1 = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name BETWEEN 'b' AND 'c'"))
        assert tb1 == [[b"2"]], tb1
        tnb1 = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name NOT BETWEEN 'b' AND 'c' ORDER BY id"))
        assert tnb1 == [[b"1"], [b"3"]], tnb1
        # AND combinations must stay correct (regression).
        anl = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name NOT LIKE 'a%' AND id > 2"))
        assert anl == [[b"3"]], anl
        abt = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE id BETWEEN 1 AND 2 AND name LIKE 'b%'"))
        assert abt == [[b"2"]], abt
        # UPDATE / DELETE with the same predicates.
        simple_query(sock, "UPDATE pred_t SET name = 'x' WHERE name NOT LIKE 'a%'")
        upn = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name = 'x' ORDER BY id"))
        assert upn == [[b"2"], [b"3"]], upn
        simple_query(sock, "UPDATE pred_t SET name = 'y' WHERE id BETWEEN 2 AND 3")
        upb = data_row_values(simple_query(
            sock, "SELECT id FROM pred_t WHERE name = 'y' ORDER BY id"))
        assert upb == [[b"2"], [b"3"]], upb
        simple_query(sock, "DELETE FROM pred_t WHERE id NOT BETWEEN 2 AND 3")
        dnb = data_row_values(simple_query(sock, "SELECT id FROM pred_t"))
        assert dnb == [[b"2"], [b"3"]], dnb

        # GROUP BY / HAVING referencing SELECT-list aliases (PG semantics):
        # "GROUP BY d" / "HAVING cnt > 1" previously returned wrong results
        # (alias unresolved: group produced all rows / having was dropped).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE grp_t (id INT PRIMARY KEY, dept TEXT, salary INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO grp_t VALUES (1,'eng',100),(2,'eng',200),(3,'ops',300)"))
        g1 = data_row_values(simple_query(
            sock, "SELECT dept AS d, COUNT(*) FROM grp_t GROUP BY d ORDER BY d"))
        assert g1 == [[b"eng", b"2"], [b"ops", b"1"]], g1
        g2 = data_row_values(simple_query(
            sock, "SELECT dept AS d FROM grp_t GROUP BY d ORDER BY d"))
        assert g2 == [[b"eng"], [b"ops"]], g2
        h1 = data_row_values(simple_query(
            sock, "SELECT dept, COUNT(*) AS cnt FROM grp_t "
            "GROUP BY dept HAVING cnt > 1"))
        assert h1 == [[b"eng", b"2"]], h1
        # Regressions: plain column GROUP BY, non-alias HAVING, ORDER BY agg.
        g3 = data_row_values(simple_query(
            sock, "SELECT dept, COUNT(*) FROM grp_t GROUP BY dept ORDER BY dept"))
        assert g3 == [[b"eng", b"2"], [b"ops", b"1"]], g3
        h2 = data_row_values(simple_query(
            sock, "SELECT dept, COUNT(*) FROM grp_t GROUP BY dept "
            "HAVING COUNT(*) > 1"))
        assert h2 == [[b"eng", b"2"]], h2
        h3 = data_row_values(simple_query(
            sock, "SELECT dept, COUNT(*) AS cnt FROM grp_t "
            "GROUP BY dept ORDER BY cnt DESC, dept"))
        assert h3 == [[b"eng", b"2"], [b"ops", b"1"]], h3

        # Aggregates over arithmetic expressions (sum/min/max/count/avg of
        # "col * 2" style args): previously returned 0 / empty because the
        # argument was not a bare column and every row was skipped.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE expr_t (id INT PRIMARY KEY, cust INT, amt INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO expr_t VALUES (1,10,100),(2,10,50),(3,20,75),(4,20,25)"))
        ag1 = data_row_values(simple_query(
            sock, "SELECT SUM(amt * 2) AS s FROM expr_t"))
        assert ag1 == [[b"500"]], ag1
        ag2 = data_row_values(simple_query(
            sock, "SELECT MIN(amt * 2) AS s FROM expr_t"))
        assert ag2 == [[b"50"]], ag2
        ag3 = data_row_values(simple_query(
            sock, "SELECT MAX(amt + 1) AS s FROM expr_t"))
        assert ag3 == [[b"101"]], ag3
        ag4 = data_row_values(simple_query(
            sock, "SELECT COUNT(amt * 2) AS s FROM expr_t"))
        assert ag4 == [[b"4"]], ag4
        ag5 = data_row_values(simple_query(
            sock, "SELECT AVG(amt * 2) AS s FROM expr_t"))
        assert ag5 == [[b"125.0000000000000000"]], ag5
        ag6 = data_row_values(simple_query(
            sock, "SELECT SUM(cust + amt) AS s FROM expr_t"))
        assert ag6 == [[b"310"]], ag6
        ag7 = data_row_values(simple_query(
            sock, "SELECT cust, SUM(amt * 2) AS s FROM expr_t "
            "GROUP BY cust ORDER BY cust"))
        assert ag7 == [[b"10", b"300"], [b"20", b"200"]], ag7
        ag8 = data_row_values(simple_query(
            sock, "SELECT cust, MIN(amt * 2) AS lo, MAX(amt * 2) AS hi FROM expr_t "
            "GROUP BY cust ORDER BY cust"))
        assert ag8 == [[b"10", b"100", b"200"], [b"20", b"50", b"150"]], ag8
        ag9 = data_row_values(simple_query(
            sock, "SELECT cust, COUNT(amt * 2) AS c FROM expr_t "
            "GROUP BY cust ORDER BY cust"))
        assert ag9 == [[b"10", b"2"], [b"20", b"2"]], ag9
        # Regressions: bare-column aggregates unchanged.
        agr1 = data_row_values(simple_query(sock, "SELECT SUM(amt) FROM expr_t"))
        assert agr1 == [[b"250"]], agr1
        agr2 = data_row_values(simple_query(
            sock, "SELECT cust, SUM(amt) FROM expr_t GROUP BY cust ORDER BY cust"))
        assert agr2 == [[b"10", b"150"], [b"20", b"100"]], agr2
        agr3 = data_row_values(simple_query(
            sock, "SELECT SUM(amt), MIN(amt), MAX(amt), COUNT(*) FROM expr_t"))
        assert agr3 == [[b"250", b"25", b"100", b"4"]], agr3

        # Decimal literals, round(x, n), nested scalar calls, POSITION(.. IN ..)
        # and decimal BETWEEN: the tokenizer used to split "3.567" into
        # "3 . 567", round ignored its precision argument, nested calls fell
        # back to literal text, position(IN) returned NULL, and decimal
        # BETWEEN bounds parsed as INF (matching no rows).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE dec_t (id INT PRIMARY KEY, v NUMERIC, name TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dec_t VALUES (1, 1.5, 'ann'), (2, 2.25, 'bob')"))
        d1 = data_row_values(simple_query(sock, "SELECT ROUND(3.567, 1) AS r"))
        assert d1 == [[b"3.6"]], d1
        d2 = data_row_values(simple_query(sock, "SELECT ABS(-5) AS a"))
        assert d2 == [[b"5"]], d2
        d3 = data_row_values(simple_query(sock, "SELECT 1.5 + 2.25 AS s"))
        assert d3 == [[b"3.75"]], d3
        d4 = data_row_values(simple_query(
            sock, "SELECT ROUND(v, 1) AS r FROM dec_t WHERE id = 2"))
        assert d4 == [[b"2.3"]], d4
        d5 = data_row_values(simple_query(
            sock, "SELECT POSITION('b' IN 'abc') AS p"))
        assert d5 == [[b"2"]], d5
        d6 = data_row_values(simple_query(
            sock, "SELECT POSITION('z' IN 'abc') AS p"))
        assert d6 == [[b"0"]], d6
        d7 = data_row_values(simple_query(
            sock, "SELECT UPPER(SUBSTRING(name, 1, 1)) AS u FROM dec_t WHERE id = 1"))
        assert d7 == [[b"A"]], d7
        d8 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE v BETWEEN 1.25 AND 2.0"))
        assert d8 == [[b"1"]], d8
        d9 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE v BETWEEN 1.6 AND 2.1"))
        assert d9 == [], d9
        d10 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE id BETWEEN 0.5 AND 1.5"))
        assert d10 == [[b"1"]], d10
        d11 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE id NOT BETWEEN 1 AND 1"))
        assert d11 == [[b"2"]], d11
        # Regression: plain IN predicate and integer-bound BETWEEN unchanged.
        d12 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE id IN (1, 2)"))
        assert d12 == [[b"1"], [b"2"]], d12
        d13 = data_row_values(simple_query(
            sock, "SELECT id FROM dec_t WHERE v BETWEEN 1 AND 2"))
        assert d13 == [[b"1"]], d13

        # Derived tables (FROM (SELECT ...)), single CTEs, and comma cross
        # joins.  Previously: the no-AS alias form "(select ...) t" crashed
        # with a substring exception; alias-qualified columns (t.id) mis-
        # replaced positions after alias stripping; aggregate/group-by/nested
        # subqueries inside FROM (...) returned empty or failed; CTE name
        # replacement rewrote letters inside keywords (a CTE named "c"
        # corrupted "select"); "from a, b" (comma cross join) was rejected.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE dt_t (id INT PRIMARY KEY, k INT, txt TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO dt_t VALUES (1,1,'aa'),(2,2,'bb'),"
                  "(3,2,'cc'),(4,3,'dd')"))
        e1 = data_row_values(simple_query(
            sock, "SELECT t.id FROM (SELECT id FROM dt_t WHERE k = 2) t"))
        assert e1 == [[b"2"], [b"3"]], e1
        e2 = data_row_values(simple_query(
            sock, "SELECT t.id FROM (SELECT id FROM dt_t WHERE k = 2) AS t"))
        assert e2 == [[b"2"], [b"3"]], e2
        e3 = data_row_values(simple_query(
            sock, "SELECT t.mx FROM (SELECT MAX(k) AS mx FROM dt_t) t"))
        assert e3 == [[b"3"]], e3
        e4 = data_row_values(simple_query(
            sock, "SELECT t.k, t.c FROM (SELECT k, COUNT(*) AS c FROM dt_t "
                  "GROUP BY k) t ORDER BY t.k LIMIT 2"))
        assert e4 == [[b"1", b"1"], [b"2", b"2"]], e4
        e5 = data_row_values(simple_query(
            sock, "SELECT u.id FROM (SELECT id FROM (SELECT id FROM dt_t "
                  "WHERE k = 2) it) u ORDER BY u.id"))
        assert e5 == [[b"2"], [b"3"]], e5
        e6 = data_row_values(simple_query(
            sock, "SELECT t.id FROM (SELECT id, k FROM dt_t) t WHERE t.k = 3"))
        assert e6 == [[b"4"]], e6
        e7 = data_row_values(simple_query(
            sock, "WITH c AS (SELECT id FROM dt_t WHERE k = 2) "
                  "SELECT id FROM c ORDER BY id"))
        assert e7 == [[b"2"], [b"3"]], e7
        e8 = data_row_values(simple_query(
            sock, "WITH c AS (SELECT MAX(k) AS mx FROM dt_t) SELECT mx FROM c"))
        assert e8 == [[b"3"]], e8
        e9 = data_row_values(simple_query(
            sock, "WITH c AS (SELECT id, k FROM dt_t) "
                  "SELECT c.id FROM c WHERE c.k = 3"))
        assert e9 == [[b"4"]], e9
        e10 = data_row_values(simple_query(
            sock, "SELECT a.id FROM dt_t a JOIN (SELECT id FROM dt_t "
                  "WHERE k = 3) b ON a.id = b.id"))
        assert e10 == [[b"4"]], e10
        # Comma cross join (PG: "from a, b" == "from a cross join b").
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE cj1 (a INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE cj2 (b INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO cj1 VALUES (1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO cj2 VALUES (5)"))
        e11 = data_row_values(simple_query(sock, "SELECT a, b FROM cj1, cj2"))
        assert e11 == [[b"1", b"5"]], e11
        e12 = data_row_values(simple_query(
            sock, "SELECT p.a, q.b FROM cj1 p, cj2 q"))
        assert e12 == [[b"1", b"5"]], e12
        # IN-list commas must NOT be rewritten by the comma-join pass.
        e13 = data_row_values(simple_query(
            sock, "SELECT id FROM dt_t WHERE id IN (1, 2) ORDER BY id"))
        assert e13 == [[b"1"], [b"2"]], e13

        # SELECT-list aliases (AS) and arithmetic projections: previously
        # "select id as no" failed with "Invalid column name id as no".
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE pa_emp (id INT PRIMARY KEY, dept TEXT, salary INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO pa_emp VALUES (1,'eng',100),(2,'eng',200),(3,'ops',300)"))
        pa1 = data_row_values(simple_query(
            sock, "SELECT id AS no FROM pa_emp ORDER BY no DESC LIMIT 2"))
        assert pa1 == [[b"3"], [b"2"]], pa1
        pa2 = data_row_values(simple_query(
            sock, "SELECT dept AS d, COUNT(*) AS c FROM pa_emp GROUP BY dept ORDER BY d"))
        assert pa2 == [[b"eng", b"2"], [b"ops", b"1"]], pa2
        pa3 = data_row_values(simple_query(
            sock, "SELECT salary * 2 AS ds FROM pa_emp WHERE id = 1"))
        assert pa3 == [[b"200"]], pa3
        pa4 = data_row_values(simple_query(
            sock, "SELECT salary + id AS s FROM pa_emp WHERE id = 2"))
        assert pa4 == [[b"202"]], pa4
        pa5 = data_row_values(simple_query(
            sock, "SELECT salary - 100 AS s FROM pa_emp WHERE id = 3"))
        assert pa5 == [[b"200"]], pa5

        # Single-table aliases and table-name qualifiers: "FROM t [as] a"
        # must resolve the table (not "t a"), and "<alias>."/"<table>."
        # qualifiers in projection/WHERE must strip to bare columns
        # (previously "emp.id = 2" silently returned zero rows).
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE al_emp (id INT PRIMARY KEY, dept TEXT, salary INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO al_emp VALUES (1,'eng',100),(2,'eng',200),(3,'ops',300)"))
        al1 = data_row_values(simple_query(
            sock, "SELECT id FROM al_emp e WHERE e.salary > 150 ORDER BY e.id"))
        assert al1 == [[b"2"], [b"3"]], al1
        al2 = data_row_values(simple_query(
            sock, "SELECT id FROM al_emp AS e WHERE e.id = 2"))
        assert al2 == [[b"2"]], al2
        al3 = data_row_values(simple_query(
            sock, "SELECT e.id FROM al_emp e WHERE e.dept = 'eng' ORDER BY e.id"))
        assert al3 == [[b"1"], [b"2"]], al3
        al4 = data_row_values(simple_query(
            sock, "SELECT al_emp.id FROM al_emp WHERE al_emp.id = 3"))
        assert al4 == [[b"3"]], al4
        al5 = data_row_values(simple_query(
            sock, "SELECT al_emp.id, al_emp.salary FROM al_emp WHERE al_emp.id = 1"))
        assert al5 == [[b"1", b"100"]], al5
        al6 = data_row_values(simple_query(
            sock, "SELECT id FROM al_emp e WHERE e.id IN (1,3)"))
        assert al6 == [[b"1"], [b"3"]], al6

        # IN / NOT IN literal lists: planner-visible conditions with
        # statistics-driven row estimates, NOT IN rewriting to AND-of-!=
        # (previously matched nothing), and correct execution.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE in_t (id INT PRIMARY KEY, v TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO in_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')"))
        simple_query(sock, "ANALYZE in_t")
        in_rows = data_row_values(simple_query(
            sock, "SELECT id FROM in_t WHERE id IN (1,3,5)"))
        assert in_rows == [[b"1"], [b"3"], [b"5"]], in_rows
        notin_rows = data_row_values(simple_query(
            sock, "SELECT id FROM in_t WHERE id NOT IN (1,2,3,4)"))
        assert notin_rows == [[b"5"]], notin_rows
        text_notin = data_row_values(simple_query(
            sock, "SELECT id FROM in_t WHERE v NOT IN ('a','b','c')"))
        assert text_notin == [[b"4"], [b"5"]], text_notin
        combo = data_row_values(simple_query(
            sock, "SELECT id FROM in_t WHERE id IN (2,4) AND v LIKE 'b'"))
        assert combo == [[b"2"]], combo
        # EXPLAIN now shows a Filter node with a reduced row estimate.
        in_exp = simple_query(
            sock, "EXPLAIN SELECT * FROM in_t WHERE id IN (1,3,5)")
        exp_text = b"".join(b for _, b in [m for m in in_exp if m[0] == b"D"])
        assert b"Filter" in exp_text, in_exp

        # JOIN with a right table whose name contains "on" (e.g.
        # "location"): the ON-clause scan must match the keyword with word
        # boundaries, not inside the table name.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE jo_loc (x INT, y INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO jo_loc VALUES (1, 2)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE jo_plain (x INT, z INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO jo_plain VALUES (1, 5)"))
        jo_rows = data_row_values(simple_query(
            sock, "SELECT x, y FROM jo_plain JOIN jo_loc ON jo_plain.x = jo_loc.x"))
        assert jo_rows == [[b"1", b"2"]], jo_rows
        # Same for a left join with an aliased "on"-bearing right table.
        jo_left = data_row_values(simple_query(
            sock, "SELECT x, y FROM jo_plain LEFT JOIN jo_loc AS l "
            "ON jo_plain.x = l.x"))
        assert jo_left == [[b"1", b"2"]], jo_left

        # JOIN keywords must have SQL-identifier boundaries.  Underscores
        # are identifier characters, so names ending in _left/_right before
        # JOIN (or merely containing _join) must not change the join type.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE kw_left (left_id INT PRIMARY KEY, k INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE kw_right (right_id INT PRIMARY KEY, k INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO kw_left VALUES (1, 7)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO kw_right VALUES (2, 7)"))
        kw_left_join = data_row_values(simple_query(
            sock, "SELECT left_id, right_id FROM kw_left JOIN kw_right "
            "ON kw_left.k = kw_right.k"))
        assert kw_left_join == [[b"1", b"2"]], kw_left_join
        kw_right_join = data_row_values(simple_query(
            sock, "SELECT right_id, left_id FROM kw_right JOIN kw_left "
            "ON kw_right.k = kw_left.k"))
        assert kw_right_join == [[b"2", b"1"]], kw_right_join
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE kw_joined (id INT PRIMARY KEY)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO kw_joined VALUES (9)"))
        kw_plain = data_row_values(simple_query(
            sock, "SELECT id FROM kw_joined"))
        assert kw_plain == [[b"9"]], kw_plain
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE kw_on (on_id INT PRIMARY KEY, k INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO kw_on VALUES (3, 1)"))
        kw_on_join = data_row_values(simple_query(
            sock, "SELECT z, on_id FROM jo_plain JOIN kw_on "
            "ON jo_plain.x = kw_on.k"))
        assert kw_on_join == [[b"5", b"3"]], kw_on_join

        # Equality joins must keep SQL NULL distinct from both another NULL
        # and a real empty string.  Previously the heap payload discarded the
        # null bitmap, so NULL = NULL and NULL = '' could both match.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE nk_a (aid INT PRIMARY KEY, k TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE nk_b (bid INT PRIMARY KEY, k TEXT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO nk_a VALUES (1, NULL), (2, ''), (3, 'x')"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO nk_b VALUES (10, NULL), (20, ''), (30, 'x')"))
        jn_left_data = data_row_values(simple_query(
            sock, "SELECT aid, k FROM nk_a"))
        assert jn_left_data == [
            [b"1", None], [b"2", b""], [b"3", b"x"]
        ], jn_left_data
        jn_right_data = data_row_values(simple_query(
            sock, "SELECT bid, k FROM nk_b"))
        assert jn_right_data == [
            [b"10", None], [b"20", b""], [b"30", b"x"]
        ], jn_right_data
        jn_inner_response = simple_query(
            sock, "SELECT aid, bid FROM nk_a JOIN nk_b "
            "ON nk_a.k = nk_b.k")
        assert not any(kind == b"E" for kind, _ in jn_inner_response), \
            jn_inner_response
        jn_inner = data_row_values(jn_inner_response)
        assert jn_inner == [[b"2", b"20"], [b"3", b"30"]], jn_inner
        jn_left_rows = data_row_values(simple_query(
            sock, "SELECT aid, bid FROM nk_a LEFT JOIN nk_b "
            "ON nk_a.k = nk_b.k"))
        assert jn_left_rows == [
            [b"1", None], [b"2", b"20"], [b"3", b"30"]
        ], jn_left_rows
        jn_right_rows = data_row_values(simple_query(
            sock, "SELECT aid, bid FROM nk_a RIGHT JOIN nk_b "
            "ON nk_a.k = nk_b.k"))
        assert jn_right_rows == [
            [None, b"10"], [b"2", b"20"], [b"3", b"30"]
        ], jn_right_rows

        # FULL OUTER JOIN is a bag operation: identical unmatched rows are
        # separate source rows and must not be collapsed during LEFT/RIGHT
        # result reconciliation.
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE fb_a (aid INT, k INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "CREATE TABLE fb_b (bid INT, k INT)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO fb_a VALUES (1, 1)"))
        assert any(kind == b"C" for kind, _ in simple_query(
            sock, "INSERT INTO fb_b VALUES (20, 2), (20, 2)"))
        fb_rows = data_row_values(simple_query(
            sock, "SELECT aid, bid FROM fb_a FULL OUTER JOIN fb_b "
            "ON fb_a.k = fb_b.k"))
        assert fb_rows == [
            [b"1", None], [None, b"20"], [None, b"20"]
        ], fb_rows

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
        constant_predicates = {
            "SELECT 1 WHERE 1 = 1": [[b"1"]],
            "SELECT 1 WHERE 1 != 2": [[b"1"]],
            "SELECT 1 WHERE 1 <> 2": [[b"1"]],
            "SELECT 1 WHERE 1 <= 1": [[b"1"]],
            "SELECT 1 WHERE 2 >= 1": [[b"1"]],
            "SELECT 1 WHERE 1 < 2": [[b"1"]],
            "SELECT 1 WHERE 2 > 1": [[b"1"]],
            "SELECT 1 WHERE 2 <= 1": [],
            "SELECT 1 WHERE 1 >= 2": [],
            "SELECT 1 WHERE 'a' < 'b'": [[b"1"]],
            "SELECT 1 WHERE 'b' <= 'a'": [],
        }
        for query, expected in constant_predicates.items():
            actual = data_row_values(simple_query(sock, query))
            assert actual == expected, (query, actual)
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
        extended_query_boolean_text_parameter(sock)
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

        # Persisting a planner change does not affect a cached plan until a
        # reload installs the value and invalidates runtime plans.
        first_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert not any(b"[plan cache hit]" in row[0]
                       for row in data_row_values(first_plan))
        second_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert any(b"[plan cache hit]" in row[0]
                   for row in data_row_values(second_plan))
        assert any(kind == b"C" for kind, _ in simple_query(
            observer_sock, "SET GLOBAL enable_seq_scan = off"))
        third_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert any(b"[plan cache hit]" in row[0]
                   for row in data_row_values(third_plan))
        reload_messages = simple_query(observer_sock, "SELECT pg_reload_conf()")
        assert data_row_values(reload_messages) == [[b"t"]], reload_messages
        fourth_plan = simple_query(observer_sock, "EXPLAIN SELECT * FROM t")
        assert not any(b"[plan cache hit]" in row[0]
                       for row in data_row_values(fourth_plan))

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
