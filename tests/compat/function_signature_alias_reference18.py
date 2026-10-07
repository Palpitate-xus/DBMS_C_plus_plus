"""Strict18 canonical SQL routine aliases; original native extension is explicit."""
import importlib.util
import socket
import uuid
from pathlib import Path

repo = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("routine_alias_runner", repo / "tests/compat/pg_diff_runner.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
client = runner.load_protocol_client()
host, port, user, database, password = runner._reference_connection_settings()
sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
client.startup_reference(sock, user, database, password)
runner.verify_reference_version(client, sock)
schema = "routine_alias_" + uuid.uuid4().hex[:20]

def query(sql, state=None, rows=None):
    result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
    print("ROUTINE_ALIAS_REFERENCE18", sql, result, flush=True)
    assert result[1] == state, (sql, result, state)
    if rows is not None:
        assert result[0] == rows, (sql, result, rows)
    return result

try:
    query("BEGIN")
    query('CREATE SCHEMA "' + schema + '"')
    query('SET LOCAL search_path TO "' + schema + '",pg_catalog')
    # Preserve the original literal-body SQL as a genuine PG syntax negative.
    # The project deliberately has this native expression-body extension.
    query("SAVEPOINT original_expression_body")
    query("CREATE FUNCTION inc(x int) RETURNS int AS 'x + 1' LANGUAGE sql", "42601")
    query("ROLLBACK TO original_expression_body")
    query("RELEASE original_expression_body")
    query("CREATE FUNCTION inc(x int) RETURNS int AS 'SELECT x + 1' LANGUAGE sql")
    query("SELECT pg_get_function_result(oid),oidvectortypes(proargtypes),prorettype::text "
          "FROM pg_proc WHERE pronamespace='" + schema + "'::regnamespace AND proname='inc'",
          rows=[["integer", "integer", "23"]])
    query("SELECT inc(4)", rows=[["5"]])
    query("CREATE FUNCTION add_alias(a int4,b INT) RETURNS integer LANGUAGE sql AS 'SELECT a+b'")
    query("SELECT oidvectortypes(proargtypes),pg_get_function_result(oid) FROM pg_proc "
          "WHERE pronamespace='" + schema + "'::regnamespace AND proname='add_alias'",
          rows=[["integer, integer", "integer"]])
    query("SELECT add_alias(20,22)", rows=[["42"]])
    query("CREATE OR REPLACE FUNCTION inc(x integer) RETURNS int4 LANGUAGE sql AS 'SELECT x + 2'")
    query("SELECT inc(3)", rows=[["5"]])
    query("SAVEPOINT changed_identity")
    query("CREATE OR REPLACE FUNCTION inc(x integer) RETURNS text LANGUAGE sql AS 'SELECT x::text'", "42P13")
    query("ROLLBACK TO changed_identity")
    query("RELEASE changed_identity")
    query("SELECT inc(3)", rows=[["5"]])
    query("ROLLBACK")
    print("[ROUTINE SIGNATURE ALIAS STRICT18] canonical identity and original extension boundary passed", flush=True)
finally:
    try:
        client.simple_query(sock, "ROLLBACK")
    finally:
        sock.close()
