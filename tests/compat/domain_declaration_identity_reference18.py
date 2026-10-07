"""Strict18 SQL domain identity/type semantics; no C++ field spelling claim."""
import importlib.util
import socket
import uuid
from pathlib import Path

repo = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("domain_identity_runner", repo / "tests/compat/pg_diff_runner.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
host, port, user, database, password = runner._reference_connection_settings()
sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
client.startup_reference(sock, user, database, password)
runner.verify_reference_version(client, sock)
schema = "domain_identity_" + uuid.uuid4().hex[:20]

def query(sql, state=None, rows=None):
    result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    print("DOMAIN_DECLARATION_REFERENCE18", sql, result, flush=True)
    assert result[1] == state, (sql, result, state)
    if rows is not None:
        assert result[0] == rows, (sql, result, rows)
    return result

try:
    query("BEGIN")
    query('CREATE SCHEMA "' + schema + '"')
    query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
    # This is the original native fixture's complete unqualified SQL.
    query("CREATE DOMAIN route_text AS VARCHAR(12) CHECK (length(VALUE) <= 12)")
    query("SELECT n.nspname,t.typname,t.typtype,t.typbasetype::text,t.typtypmod::text "
          "FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace "
          "WHERE n.nspname='" + schema + "' AND t.typname='route_text'",
          rows=[[schema, "route_text", "d", "1043", "16"]])
    query("SELECT 'hello'::route_text,NULL::route_text", rows=[["hello", None]])
    query('SELECT \'hello\'::"' + schema + '"."route_text"', rows=[["hello"]])
    query("SAVEPOINT duplicate_identity")
    query("CREATE DOMAIN route_text AS INT", "42710")
    query("ROLLBACK TO duplicate_identity")
    query("RELEASE duplicate_identity")
    query("SELECT 'hello'::route_text", rows=[["hello"]])
    query("DROP DOMAIN route_text")
    query("SELECT count(*)::text FROM pg_type WHERE typnamespace='" + schema +
          "'::regnamespace AND typname='route_text'", rows=[["0"]])
    query("ROLLBACK")
    print("[DOMAIN DECLARATION STRICT18] actual namespace/type/modifier/cast/drop identity passed", flush=True)
finally:
    try:
        client.simple_query(sock, "ROLLBACK")
    finally:
        sock.close()
