#!/usr/bin/env python3
"""Canonical SQL identifiers and REGTYPE scalar names, independent of enum arrays."""
from compat.numeric_type_review import Review
from search_path_values_protocol_e2e_test import KEYWORDS
import struct
import uuid

SPECIALS = [
    ("", '""'), ("plain", "plain"), ("_", "_"), ("_a9", "_a9"),
    ("Upper", '"Upper"'), ("A", '"A"'), ("1a", '"1a"'), ("a$b", '"a$b"'),
    ("a.b", '"a.b"'), ("a b", '"a b"'), ("a,b", '"a,b"'),
    ('a"b', '"a""b"'), ("a'b", '"a\'b"'), ("line\nbreak", '"line\nbreak"'),
    ("tab\tname", '"tab\tname"'), ("汉", '"汉"'), ("é", '"é"'),
    ("rank[]", '"rank[]"'), ("a\\b", '"a\\b"'), ("a;z", '"a;z"'),
    ("a\n", '"a\n"'), ("a"*63, "a"*63),
]
TYPE_NAMES = [
    ("plain", "plain"), ("_lead", "_lead"), ("foo9", "foo9"), ("Upper", '"Upper"'),
    ("select", '"select"'), ("between", '"between"'), ("view", "view"),
    ("a$b", '"a$b"'), ('a"b', '"a""b"'), ("a.b", '"a.b"'),
    ("a b", '"a b"'), ("rank[]", '"rank[]"'), ("汉", '"汉"'), ("a'b", '"a\'b"'),
]


def ident(value):
    return '"'+value.replace('"', '""')+'"'


def literal(value):
    if "\\" in value:
        return "E'"+value.replace("\\", "\\\\").replace("'", "''")+"'"
    return "'"+value.replace("'", "''")+"'"


def main():
    review = Review("REGTYPE_IDENTIFIER_PROTOCOL")
    prefix = "r152_"+uuid.uuid4().hex[:8]
    schemas = (prefix, prefix+"_other", prefix+" Space")
    created = []

    def request(sql, mode="simple"):
        if mode == "simple":
            return review.client.simple_query(review.sock, sql)
        packet = review.client.typed(b"P", b"\0"+sql.encode()+b"\0"+struct.pack("!H", 0))
        packet += review.client.typed(b"B", b"\0\0"+struct.pack("!HHHH", 0, 0, 1, mode))
        packet += review.client.typed(b"D", b"P\0")
        packet += review.client.typed(b"E", b"\0"+struct.pack("!I", 0))
        review.sock.sendall(packet+review.client.typed(b"S"))
        return review.client.read_until_ready(review.sock)

    def text(sql, values, mode="simple"):
        messages = request(sql, mode)
        format_code = 0 if mode == "simple" else mode
        fields = review.client.row_description_fields(messages)
        rows = review.client.data_row_values(messages)
        wanted = [[None if value is None else value.encode() for value in values]]
        good = rows == wanted and len(fields) == len(values)
        good = good and all((field[3], field[4], field[5], field[6]) ==
                            (25, -1, -1, format_code) for field in fields)
        good = good and [body for kind, body in messages if kind == b"C"] == [b"SELECT 1\0"]
        good = good and messages[-1] == (b"Z", b"I")
        good = good and not any(kind in (b"E", b"N") for kind, _ in messages)
        review.record((sql, mode), good, (rows, fields, messages[-1]), wanted)

    def utility(sql, tag=None):
        messages = request(sql)
        result = review.runner.decode_wire_result(messages, include_types=True)
        good = result[1] is None and result[0] == [] and messages[-1] == (b"Z", b"I")
        good = good and [body for kind, body in messages if kind == b"C"] == [
            (tag or sql.split()[0]).encode()+b"\0"]
        good = good and not any(kind in (b"T", b"D", b"E") for kind, _ in messages)
        review.record(sql, good, result, tag)

    def declared(space, name):
        return ident(space)+"."+ident(name)

    def output(space, name, expected):
        target = declared(space, name)
        sql = ("SELECT pg_typeof(NULL::"+target+")::text AS v, "+
               literal(target)+"::regtype::text AS r")
        for mode in ("simple", 0, 1):
            text(sql, [expected, expected], mode)

    try:
        assert len(KEYWORDS) == 494
        for item in KEYWORDS:
            name, category = item.split(":")
            expected = name if category == "U" else ident(name)
            sql = "SELECT quote_ident("+literal(name)+") AS q, format('%I',"+literal(name)+") AS f"
            for mode in ("simple", 0, 1):
                text(sql, [expected, expected], mode)
        for name, expected in SPECIALS:
            sql = "SELECT quote_ident("+literal(name)+") AS q, format('%I',"+literal(name)+") AS f"
            for mode in ("simple", 0, 1):
                text(sql, [expected, expected], mode)
        for mode in ("simple", 0, 1):
            text("SELECT quote_ident(NULL::text) AS q", [None], mode)
        review.query("SELECT format('%I',NULL::text)", state="22004")
        for space in schemas:
            utility("CREATE SCHEMA "+ident(space), "CREATE SCHEMA")
            created.append(space)
        for name, display in TYPE_NAMES:
            utility("CREATE TYPE "+declared(prefix, name)+" AS ENUM('10','2')", "CREATE TYPE")
            output(prefix, name, prefix+"."+display)
        utility("SET search_path TO "+ident(prefix)+", pg_catalog")
        for name, display in TYPE_NAMES:
            output(prefix, name, display)
        utility("CREATE TYPE "+declared(schemas[1], "plain")+" AS ENUM('10','2')", "CREATE TYPE")
        utility("SET search_path TO "+ident(schemas[1])+","+ident(prefix)+",pg_catalog")
        output(prefix, "plain", prefix+".plain")
        output(schemas[1], "plain", "plain")
        utility("SET search_path TO pg_catalog")
        utility("CREATE TYPE "+declared(schemas[2], "plain")+" AS ENUM('10','2')", "CREATE TYPE")
        output(schemas[2], "plain", ident(schemas[2])+".plain")
        utility("SET search_path TO "+ident(schemas[2])+",pg_catalog")
        output(schemas[2], "plain", "plain")
        utility("SET search_path TO public")
    finally:
        try:
            for space in reversed(created):
                utility("DROP SCHEMA "+ident(space)+" CASCADE", "DROP SCHEMA")
        finally:
            review.close()
    expected = 3*(494+len(SPECIALS))+4+3+len(TYPE_NAMES)*4+1+len(TYPE_NAMES)*3+2+6+2+3+1+3+1+3
    review.complete(expected)


if __name__ == "__main__":
    main()
