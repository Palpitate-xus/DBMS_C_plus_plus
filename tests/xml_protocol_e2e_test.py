#!/usr/bin/env python3
"""XML OID metadata, validation, functions, and binary wire I/O."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "xml_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        messages = client.simple_query(server["sock"], sql)
        return messages, runner.decode_wire_result(messages, include_types=True)

    def expect_error(sql, expected):
        _, decoded = execute(sql)
        rows, state, message, _, _, _ = decoded
        assert rows == [] and state == expected, (sql, state, message, rows)

    try:
        _, decoded = execute(
            "CREATE TABLE xml_wire (id INT PRIMARY KEY, payload XML);")
        assert decoded[1] is None, decoded
        _, decoded = execute(
            "INSERT INTO xml_wire VALUES "
            "(1, XML '<a xmlns:p=\"urn:t\"><p:b>&amp;</p:b></a>'), "
            "(2, XML '<x/><y/>');")
        assert decoded[1] is None, decoded

        messages, decoded = execute(
            "SELECT payload FROM xml_wire ORDER BY id;")
        rows, state, message, headers, tag, type_oids = decoded
        assert state is None, message
        assert rows == [
            ['<a xmlns:p="urn:t"><p:b>&amp;</p:b></a>'],
            ["<x/><y/>"]], rows
        assert headers == ["payload"] and tag == "SELECT 2"
        assert type_oids == [142], type_oids
        fields = client.row_description_fields(messages)
        assert fields[0][3:] == (142, -1, -1, 0), fields[0]

        _, decoded = execute(
            "SELECT xml_is_well_formed_content('<a/><b/>') AS c, "
            "xml_is_well_formed_document('<a/><b/>') AS d, "
            "xmlconcat(XML '<a/>', NULL, XML '<b/>') AS joined;")
        rows, state, message, _, tag, type_oids = decoded
        assert state is None, message
        assert rows == [["t", "f", "<a/><b/>"]], rows
        assert type_oids == [16, 16, 142], type_oids
        assert tag == "SELECT 1", tag

        expect_error("SELECT '<a>&undefined;</a>'::xml;", "2200N")
        raw = b'<wire attr="&amp;">ok</wire>'
        client.extended_query_binary_parameter(
            server["sock"], "xml", "SELECT $1::xml", 142, raw, raw)
        print("[XML PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
