#!/usr/bin/env python3
"""Project logical preview: typed JSON cells preserve NULL and field boundaries."""
import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("logical_tuple_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = runner.start_ours(client)
    checks = 0; failures = []

    def query(sql):
        result = runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)
        assert result[1] is None, (sql, result)
        return result

    def literal(value):
        return "NULL" if value is None else "'" + value.replace("'", "''") + "'"

    def peek(expected, operation, table="tuple_wire"):
        nonlocal checks
        result = query("SHOW LOGICAL CHANGES FOR SLOT tuple_wire_slot")
        checks += 1; parsed = []
        try:
            for row in result[0]:
                # Metadata belongs to the outer message, not to any field.
                text = row[0]; prefix = "table " + table + ": " + operation + ": "
                assert text.startswith(prefix) and text.endswith(")\n")
                payload = text[len(prefix):].rsplit(" (xid ", 1)[0]
                decoder = json.JSONDecoder()
                def cells(encoded):
                    assert encoded.startswith("DBMS_TUPLE_V1:")
                    value, end = decoder.raw_decode(encoded[len("DBMS_TUPLE_V1:"):])
                    return value, end + len("DBMS_TUPLE_V1:")
                if operation == "UPDATE":
                    assert payload.startswith("old-key ")
                    old, end = cells(payload[len("old-key "):])
                    tail = payload[len("old-key ") + end:]
                    assert tail.startswith(" new-tuple ")
                    new, end = cells(tail[len(" new-tuple "):])
                    assert end == len(tail) - len(" new-tuple ")
                    parsed.append([old, new])
                else:
                    if operation == "DELETE":
                        assert payload.startswith("old-key "); payload = payload[len("old-key "):]
                    value, end = cells(payload); assert end == len(payload); parsed.append(value)
            assert result[3] == ["data"] and result[5] == [25]
            assert result[4] == "SELECT " + str(len(expected)) and parsed == expected
            passed = True
        except (AssertionError, ValueError):
            passed = False
        print("LOGICAL_TUPLE_WIRE", operation, "check", checks, "PASS", passed, flush=True)
        if not passed: failures.append((checks, operation, result, expected))

    def confirm():
        query("SHOW LOGICAL CONFIRM FOR SLOT tuple_wire_slot")

    def image(id_, pair):
        return [["int", str(id_)], ["text", pair[0]], ["text", pair[1]]]

    try:
        query("SET compatibility_mode = extended")
        query("CREATE TABLE tuple_wire(id INT PRIMARY KEY,a TEXT,b TEXT)")
        query("CREATE PUBLICATION tuple_wire_pub FOR TABLE tuple_wire")
        query("CREATE REPLICATION SLOT tuple_wire_slot LOGICAL dbms_test_decoding")
        cases = [(None, "x"), ("", "x"), ("a|b", "c"), ("a", "b|c"),
                 ("NULL", None), ('quote"', "back\\slash"), ("line\nnext", "\r\t"),
                 ("中文", "é"), (None, None), ("", "")]
        for i, pair in enumerate(cases, 1):
            query(f"INSERT INTO tuple_wire VALUES({i},{literal(pair[0])},{literal(pair[1])})")
            peek([image(i, pair)], "INSERT"); peek([image(i, pair)], "INSERT"); confirm()
            replacement = cases[i % len(cases)]
            query(f"UPDATE tuple_wire SET a={literal(replacement[0])},b={literal(replacement[1])} WHERE id={i}")
            peek([[image(i, pair), image(i, replacement)]], "UPDATE"); confirm()
            query(f"DELETE FROM tuple_wire WHERE id={i}")
            peek([image(i, replacement)], "DELETE"); confirm()
        query("BEGIN")
        query("INSERT INTO tuple_wire VALUES(99,NULL,'rollback|')")
        peek([], "INSERT"); query("ROLLBACK"); peek([], "INSERT")
        query("BEGIN")
        query("INSERT INTO tuple_wire VALUES(99,'','commit|')")
        peek([], "INSERT"); query("COMMIT")
        peek([image(99, ("", "commit|"))], "INSERT"); confirm()
        query("CREATE TABLE tuple_generated(id INT PRIMARY KEY,a TEXT,n INT GENERATED ALWAYS AS(length(a)) VIRTUAL,v TEXT GENERATED ALWAYS AS(coalesce(a,'fallback')) VIRTUAL)")
        query("ALTER PUBLICATION tuple_wire_pub ADD TABLE tuple_generated")
        for i, value in enumerate((None, "", "x|"), 1):
            generated = [["int", str(i)], ["text", value],
                         ["int", None if value is None else str(len(value))],
                         ["text", "fallback" if value is None else value]]
            query(f"INSERT INTO tuple_generated(id,a) VALUES({i},{literal(value)})")
            peek([generated], "INSERT", "tuple_generated"); confirm()
            query(f"UPDATE tuple_generated SET a={literal(value)} WHERE id={i}")
            peek([[generated, generated]], "UPDATE", "tuple_generated"); confirm()
            query(f"DELETE FROM tuple_generated WHERE id={i}")
            peek([generated], "DELETE", "tuple_generated"); confirm()
        query("DROP REPLICATION SLOT tuple_wire_slot")
        query("DROP PUBLICATION tuple_wire_pub")
        query("DROP TABLE tuple_wire")
        query("DROP TABLE tuple_generated")
        print("LOGICAL_TUPLE_WIRE_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 53 and not failures, failures
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
