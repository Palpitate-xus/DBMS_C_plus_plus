#!/usr/bin/env python3
"""EXPLAIN ANALYZE executes one prepared, typed operator tree.

These include the original P0002/22P02 failures, irreversible sequence
observations, real writer rollback, and demand on either side of Sort.
"""
import importlib.util
import json
from pathlib import Path
import struct


SETUP = [
    "CREATE TEMP TABLE explain_typed_rows(id INT,payload TEXT);",
    "INSERT INTO explain_typed_rows VALUES(1,'11'),(2,'22'),(3,'bad');",
    'CREATE TEMP TABLE explain_typed_names(id INT,"ID" INT,label TEXT);',
    "INSERT INTO explain_typed_names VALUES(1,7,''),(2,NULL,'NULL');",
    "CREATE TEMP TABLE explain_typed_arrays(id INT,items INT[]);",
    "INSERT INTO explain_typed_arrays VALUES(1,ARRAY[1,2]),(2,ARRAY[3]);",
    "CREATE TEMP TABLE explain_typed_sink(id INT PRIMARY KEY);",
    "CREATE TEMP TABLE explain_typed_parent(id INT PRIMARY KEY);",
    "CREATE TEMP TABLE explain_typed_child(id INT PRIMARY KEY,pid INT,CONSTRAINT explain_typed_fk FOREIGN KEY(pid) REFERENCES explain_typed_parent(id) DEFERRABLE INITIALLY DEFERRED);",
    "CREATE SEQUENCE explain_typed_seq START 1;",
    "CREATE FUNCTION explain_typed_counted(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE q BIGINT; BEGIN SELECT nextval('explain_typed_seq') INTO q; RETURN arg; END; $$;",
    "CREATE FUNCTION explain_typed_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE q BIGINT; BEGIN SELECT nextval('explain_typed_seq') INTO q; INSERT INTO explain_typed_sink VALUES(arg); RETURN -arg; END; $$;",
    "CREATE FUNCTION explain_typed_strict(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO explain_typed_sink VALUES(arg) RETURNING id) SELECT id INTO STRICT n FROM explain_typed_sink WHERE id=-1; RETURN n; END; $$;",
    "CREATE FUNCTION explain_typed_cast(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN WITH ins AS(INSERT INTO explain_typed_sink VALUES(arg) RETURNING id) SELECT 'bad' INTO n; RETURN n; END; $$;",
    "CREATE FUNCTION explain_typed_deferred(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE q BIGINT; BEGIN SELECT nextval('explain_typed_seq') INTO q; INSERT INTO explain_typed_child VALUES(arg,999); RETURN arg; END; $$;",
]

# name, SQL, SQLSTATE, actual output rows, irreversible calls, surviving writes
CASES = [
    ("original_fromless_error", "SELECT explain_typed_strict(93)", "P0002", None, 0, []),
    ("original_table_error", "SELECT explain_typed_cast(id) FROM explain_typed_rows", "22P02", None, 0, []),
    ("fromless", "SELECT explain_typed_counted(7)", None, 1, 1, []),
    ("table", "SELECT explain_typed_counted(id) FROM explain_typed_rows", None, 3, 3, []),
    ("limited", "SELECT explain_typed_counted(id) FROM explain_typed_rows LIMIT 1", None, 1, 1, []),
    ("physical_sort", "SELECT explain_typed_counted(id) FROM explain_typed_rows ORDER BY id DESC LIMIT 1", None, 1, 1, []),
    ("target_sort", "SELECT explain_typed_counted(id) FROM explain_typed_rows ORDER BY explain_typed_counted(id) DESC LIMIT 1", None, 1, 3, []),
    ("qualified_target_sort", "SELECT explain_typed_counted(id) FROM explain_typed_rows r ORDER BY explain_typed_counted(r.id) DESC LIMIT 1", None, 1, 3, []),
    ("alias_sort", "SELECT explain_typed_counted(id) AS n FROM explain_typed_rows ORDER BY n LIMIT 1", None, 1, 3, []),
    ("ordinal_sort", "SELECT explain_typed_counted(id) FROM explain_typed_rows ORDER BY 1 LIMIT 1", None, 1, 3, []),
    ("offset", "SELECT explain_typed_counted(id) FROM explain_typed_rows OFFSET 2 LIMIT 1", None, 1, 3, []),
    ("sorted_offset", "SELECT explain_typed_counted(id) FROM explain_typed_rows ORDER BY id OFFSET 2 LIMIT 1", None, 1, 3, []),
    ("zero_sort_offset", "SELECT explain_typed_counted(id) FROM explain_typed_rows ORDER BY explain_typed_counted(id) OFFSET 2 LIMIT 0", None, 0, 0, []),
    ("zero_fromless", "SELECT explain_typed_strict(93) LIMIT 0", None, 0, 0, []),
    ("empty", "SELECT explain_typed_strict(id) FROM explain_typed_rows WHERE FALSE", None, 0, 0, []),
    ("lazy_case", "SELECT CASE WHEN id=1 THEN explain_typed_counted(id) ELSE 0 END FROM explain_typed_rows ORDER BY id", None, 3, 1, []),
    ("writer", "SELECT explain_typed_writer(id) FROM explain_typed_rows", None, 3, 3, [["1"], ["2"], ["3"]]),
    ("writer_limit", "SELECT explain_typed_writer(id) FROM explain_typed_rows ORDER BY id DESC LIMIT 1", None, 1, 1, [["3"]]),
    ("writer_shared_target", "SELECT explain_typed_writer(id) FROM explain_typed_rows ORDER BY explain_typed_writer(id) LIMIT 1", None, 1, 3, [["1"], ["2"], ["3"]]),
    ("later_projection_error", "SELECT explain_typed_writer(id),CAST(payload AS INT) FROM explain_typed_rows", "22P02", None, 3, []),
    ("immutable_sort_projection", "SELECT CAST(payload AS INT) FROM explain_typed_rows ORDER BY id LIMIT 1", "22P02", None, 0, []),
    ("mixed_sort_projection", "SELECT explain_typed_counted(id),CAST(payload AS INT) FROM explain_typed_rows ORDER BY id LIMIT 1", "22P02", None, 0, []),
    ("volatile_qual", "SELECT explain_typed_counted(id) FROM explain_typed_rows WHERE explain_typed_counted(id)>0 LIMIT 1", None, 1, 2, []),
    ("quoted_cells", 'SELECT id,"ID",CASE WHEN label IS NULL THEN 0 ELSE length(label) END FROM explain_typed_names ORDER BY id', None, 2, 0, []),
    ("metadata_before_writer", "SELECT explain_typed_writer(id),missing_value FROM explain_typed_rows WHERE FALSE", "42703", None, 0, []),
    ("unknown_function_before_writer", "SELECT explain_typed_writer(id),explain_no_such_function(id) FROM explain_typed_rows", "42883", None, 0, []),
    ("distinct", "SELECT DISTINCT explain_typed_counted(id)%2 FROM explain_typed_rows LIMIT 1", None, 1, 3, []),
    ("qualified_star", "SELECT r.* FROM explain_typed_rows r ORDER BY r.id DESC OFFSET 1 LIMIT 1", None, 1, 0, []),
    ("fromless_star", "SELECT *", "42601", None, 0, []),
    ("deferred_commit_error", "SELECT explain_typed_deferred(1)", "23503", None, 1, []),
    ("scalar_fromless", "SELECT (SELECT explain_typed_counted(7))", None, 1, 1, []),
    ("scalar_initplan", "SELECT (SELECT explain_typed_counted(7)) FROM explain_typed_rows", None, 3, 1, []),
    ("scalar_lazy_case", "SELECT CASE WHEN FALSE THEN (SELECT explain_typed_writer(7)) ELSE 0 END FROM explain_typed_rows", None, 3, 0, []),
    ("scalar_strict_error", "SELECT (SELECT explain_typed_strict(93)) FROM explain_typed_rows", "P0002", None, 0, []),
    ("scalar_cast_error", "SELECT (SELECT explain_typed_cast(93)) FROM explain_typed_rows", "22P02", None, 0, []),
    ("scalar_cardinality", "SELECT (SELECT id FROM explain_typed_rows)", "21000", None, 0, []),
    ("scalar_empty", "SELECT (SELECT CAST(payload AS INT) FROM explain_typed_rows WHERE FALSE)", None, 1, 0, []),
    ("canonical_integer_key", "SELECT explain_typed_counted(1) FROM explain_typed_rows ORDER BY explain_typed_counted(01) LIMIT 1", None, 1, 3, []),
    ("canonical_cast_key", "SELECT explain_typed_counted(CAST(id AS INT)) FROM explain_typed_rows ORDER BY explain_typed_counted(CAST(id AS INTEGER)) LIMIT 1", None, 1, 3, []),
    ("array_projection", "SELECT items,array_length(items,1) FROM explain_typed_arrays ORDER BY id", None, 2, 0, []),
    ("scalar_correlated", "SELECT (SELECT explain_typed_counted(r.id)) FROM explain_typed_rows r", None, 3, 3, []),
    ("scalar_nested_correlated", "SELECT (SELECT (SELECT explain_typed_counted(r.id))) FROM explain_typed_rows r", None, 3, 3, []),
    ("fromless_null", "SELECT NULL", None, 1, 0, []),
    ("constant_distinct", "SELECT DISTINCT 1 FROM explain_typed_rows", None, 1, 0, []),
    ("null_distinct", "SELECT DISTINCT NULL FROM explain_typed_rows", None, 1, 0, []),
    ("null_distinct_alias", "SELECT DISTINCT NULL AS missing FROM explain_typed_rows", None, 1, 0, []),
]


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("typed_explain_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    failures = []

    def query(sql):
        messages = client.simple_query(server["sock"], sql)
        return runner.decode_wire_result(messages, include_types=True)

    def expect(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)
        return result[0]

    def check(name, condition, detail):
        if condition:
            print("[EXPLAIN TYPED]", name, "PASS", flush=True)
        else:
            failures.append((name, detail))
            print("[EXPLAIN TYPED]", name, "FAIL", detail, flush=True)

    try:
        for sql in SETUP:
            expect(sql)
        for format_ in ("TEXT", "JSON"):
            for name, sql, state, rows, calls, writes in CASES:
                expect("DELETE FROM explain_typed_sink;")
                expect("ALTER SEQUENCE explain_typed_seq RESTART WITH 1;")
                result = query(f"EXPLAIN (ANALYZE TRUE,FORMAT {format_},TIMING FALSE) {sql};")
                sink = expect("SELECT id FROM explain_typed_sink ORDER BY id;")
                child = expect("SELECT id FROM explain_typed_child;")
                next_value = expect("SELECT nextval('explain_typed_seq');")
                detail = (result, sink, next_value)
                check(format_ + "/" + name + "/effects", result[1] == state and sink == writes and child == [] and
                    next_value == [[str(calls + 1)]], detail)
                if state is not None:
                    check(format_ + "/" + name + "/no_partial_plan", result[0] == [], result)
                elif result[1] is None:
                    text = "\n".join(row[0] for row in result[0])
                    if format_ == "JSON":
                        try:
                            document = json.loads(text)
                            check(format_ + "/" + name + "/rows", document["actualRows"] == rows, document)
                        except (ValueError, KeyError) as error:
                            check(format_ + "/" + name + "/rows", False, (text, str(error)))
                    else:
                        check(format_ + "/" + name + "/rows", "Actual rows: " + str(rows) in text.splitlines(), text)
        for format_ in ("TEXT", "JSON"):
            expect("DELETE FROM explain_typed_sink;")
            expect("ALTER SEQUENCE explain_typed_seq RESTART WITH 1;")
            for _ in range(2):
                result = query(f"EXPLAIN (FORMAT {format_}) SELECT explain_typed_writer(id) FROM explain_typed_rows;")
                check(format_ + "/plain_no_effect", result[1] is None and
                      expect("SELECT id FROM explain_typed_sink;") == [], result)
            check(format_ + "/plain_no_sequence", expect("SELECT nextval('explain_typed_seq');") == [["1"]], "plain called volatile target")
        # Repeating an analyzed, cacheable statement must execute it again.
        expect("ALTER SEQUENCE explain_typed_seq RESTART WITH 1;")
        for _ in range(2):
            expect("EXPLAIN ANALYZE SELECT explain_typed_counted(id) FROM explain_typed_rows;")
        check("analyze_repeat_executes", expect("SELECT nextval('explain_typed_seq');") == [["7"]], "cached ANALYZE skipped execution")
        # Parse/Bind must remain pure; ANALYZE executes on the portal's Execute
        # message, with the same commit/publication/error boundary as simple Q.
        expect("DELETE FROM explain_typed_sink;")
        expect("ALTER SEQUENCE explain_typed_seq RESTART WITH 1;")
        parse = b"explain_typed_extended\0EXPLAIN (ANALYZE TRUE,FORMAT JSON,TIMING FALSE) SELECT explain_typed_writer(id) FROM explain_typed_rows\0" + struct.pack("!H", 0)
        bind = b"explain_typed_portal\0explain_typed_extended\0" + struct.pack("!HHH", 0, 0, 0)
        server["sock"].sendall(client.typed(b"P", parse) + client.typed(b"B", bind) + client.typed(b"S"))
        prepared = runner.decode_wire_result(client.read_until_ready(server["sock"]), include_types=True)
        check("extended_parse_bind_pure", prepared[1] is None and
              expect("SELECT id FROM explain_typed_sink;") == [] and
              expect("SELECT nextval('explain_typed_seq');") == [["1"]], prepared)
        expect("ALTER SEQUENCE explain_typed_seq RESTART WITH 1;")
        # A portal does not survive its implicit transaction's end at Sync.
        # The named statement does; re-bind it for this real execution.
        bind = b"explain_typed_execution_portal\0explain_typed_extended\0" + struct.pack("!HHH", 0, 0, 0)
        server["sock"].sendall(client.typed(b"B", bind) +
            client.typed(b"E", b"explain_typed_execution_portal\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        executed = runner.decode_wire_result(client.read_until_ready(server["sock"]), include_types=True)
        check("extended_analyze_executes", executed[1] is None and
              expect("SELECT id FROM explain_typed_sink ORDER BY id;") == [["1"], ["2"], ["3"]] and
              expect("SELECT nextval('explain_typed_seq');") == [["4"]], executed)
        expect("DELETE FROM explain_typed_sink;")
        parse = b"explain_typed_failed\0EXPLAIN (ANALYZE TRUE,FORMAT JSON) SELECT explain_typed_strict(93)\0" + struct.pack("!H", 0)
        bind = b"explain_typed_error_portal\0explain_typed_failed\0" + struct.pack("!HHH", 0, 0, 0)
        server["sock"].sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
            client.typed(b"E", b"explain_typed_error_portal\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        failed = runner.decode_wire_result(client.read_until_ready(server["sock"]), include_types=True)
        check("extended_error_no_plan_rollback", failed[1] == "P0002" and failed[0] == [] and
              expect("SELECT id FROM explain_typed_sink;") == [], failed)
        expect("SELECT 42;")
        assert not failures, f"{len(failures)} EXPLAIN typed execution assertions failed"
        print("[EXPLAIN TYPED] all controls passed", flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
