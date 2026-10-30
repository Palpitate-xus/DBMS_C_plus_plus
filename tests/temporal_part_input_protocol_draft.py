#!/usr/bin/env python3
"""Actual PG temporal input signatures, grammar and empty-table eligibility."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import json
from pathlib import Path
import struct
import sys
import uuid


def main():
    if sys.argv[1:] == ["--reference"]:
        sys.argv[1:] = ["--reference18"]
    review = Review("TEMPORAL_PART_INPUT_PROTOCOL")
    client, sock = review.client, review.sock
    corpus = json.loads((Path(__file__).parent / "compat/temporal_part_input_pg18_expected.json").read_text())
    assert corpus["version"] == "180006" and len(corpus["cases"]) == 948
    schema = "temporal_contract_" + uuid.uuid4().hex
    table = identifier(schema) + ".rows_"
    owned = False

    def operation(mode, sql, expected, inputs=(), values=(), input_binary=False):
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
            state, fields, parse_count = expected['state'], expected['simpleFields'], None
            wanted = [[None if value is None else value.encode() for value in row] for row in expected['rows']]
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(inputs))
            parse += b''.join(struct.pack('!I', oid) for oid in inputs)
            packet = client.typed(b'P', parse)
            if mode == 'describe':
                state, fields, parse_count = expected['describeState'], expected['describeFields'], expected['describeParseComplete']
                wanted = []
                packet += client.typed(b'D', b'S\0')
            else:
                spec = expected[mode]
                state, fields, parse_count = spec['state'], spec['fields'], spec['parseComplete']
                wanted = [[None if value is None else bytes.fromhex(value) for value in row] for row in spec['rows']]
                bind = b'\0\0' + struct.pack('!HHH', 1, int(input_binary), len(values))
                for value in values:
                    raw = None if value is None else value if isinstance(value, bytes) else value.encode()
                    bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                bind += struct.pack('!HH', 1, int(mode == 'binary'))
                packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        actual_rows, valid = strict_data_rows(messages)
        states = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        actual_fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = valid and states == ([state] if state else []) and actual_rows == wanted
        good = good and [list(field[3:]) for field in actual_fields] == fields
        good = good and tags == ([] if state or mode == 'describe' else ['SELECT ' + str(len(wanted))])
        good = good and messages[-1:] == [(b'Z', b'I')]
        if parse_count is not None:
            good = good and sum(kind == b'1' for kind, _ in messages) == parse_count
            good = good and sum(kind == b'2' for kind, _ in messages) == (0 if state or mode == 'describe' else 1)
        if mode == 'describe':
            good = good and not any(kind in (b'2', b'D', b'C') for kind, _ in messages)
        review.record((mode, sql, inputs, input_binary), good, (actual_rows, states, actual_fields, tags), expected)

    def declaration(function, invalid=False, empty=False):
        oid, size = (1700, -1) if function == "extract" else (701, 8)
        state = "42883" if invalid else None
        fields = [] if invalid else [[oid, size, -1, 0]]
        expected = dict(state=state, rows=[] if invalid or empty else [[None]], simpleFields=fields,
                        describeState=state, describeFields=fields, describeParseComplete=int(not invalid))
        for mode in ("text", "binary"):
            expected[mode] = dict(state=state, rows=[] if invalid or empty else [[None]],
                                  fields=[] if invalid else [[oid, size, -1, int(mode == "binary")]], parseComplete=int(not invalid))
        return expected

    try:
        for row in corpus["cases"]:
            for mode in ("simple", "text", "binary", "describe"):
                operation(mode, row["sql"], row["expected"])
        before = len(review.failures)
        review.query("CREATE SCHEMA " + identifier(schema), rows=[], tag="CREATE SCHEMA")
        assert len(review.failures) == before, "private schema ownership not confirmed"
        owned = True
        review.query("CREATE TABLE " + table + "(id integer,n integer,b boolean,a interval[],txt text,d date,tm time,z timetz,ts timestamp,tz timestamptz,i interval)", rows=[], tag="CREATE TABLE")
        review.query("INSERT INTO " + table + "(id) VALUES(1),(2)", rows=[], tag="INSERT 0 2")
        forms = (("extract", "grammar"), ("extract", '"extract"'), ("extract", "pg_catalog.extract"),
                 ("date_part", "date_part"), ("date_part", '"date_part"'), ("date_part", "pg_catalog.date_part"))
        for function, form in forms:
            for column in ("n", "b", "a", "txt", "d", "tm", "z", "ts", "tz", "i"):
                expression = "EXTRACT('year' FROM " + column + ")" if form == "grammar" else form + "('year'," + column + ")"
                for suffix in (" WHERE id=2", " WHERE false", " LIMIT 0"):
                    expected = declaration(function, column in ("n", "b", "a", "txt"), suffix != " WHERE id=2")
                    for mode in ("simple", "text", "binary", "describe"):
                        operation(mode, "SELECT " + expression + " AS v FROM " + table + suffix, expected)
        review.query("DROP SCHEMA " + identifier(schema) + " CASCADE", rows=[], tag="DROP SCHEMA")
        owned = False
        review.complete(4516)
    finally:
        if owned:
            client.simple_query(sock, "DROP SCHEMA " + identifier(schema) + " CASCADE")
        review.close()


if __name__ == "__main__":
    main()
