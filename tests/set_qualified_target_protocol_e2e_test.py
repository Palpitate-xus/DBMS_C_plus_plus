#!/usr/bin/env python3
"""Metadata-only qualified SET target grammar; Parse must not execute a setting."""
from compat.numeric_type_review import Review
from search_path_values_protocol_e2e_test import KEYWORDS
import struct

# PG 18.6 kwlist.h TYPE_FUNC_NAME_KEYWORD, excluded from ColId.
TYPE_FUNCTION_NAMES = frozenset(["authorization","binary","collation","concurrently","cross","current_schema","freeze","full","ilike","inner","is","isnull","join","left","like","natural","notnull","outer","overlaps","right","similar","tablesample","verbose"])
VALID = [
    "SET search_path.extra TO public,pg_catalog",
    "SET compatibility_mode.extra = 'extended'",
    "SET r157.foo.bar TO 'one'",
    "SET r157 . foo . bar = 'one'",
    "SET \"r157\".\"Upper\" TO 'one'",
    "SET r157.\"a.b\" TO 'one'",
    "SET \"r157.x\".a TO 'one'",
    "SET r157.\"a\"\"b\" TO 'one'",
    "SET r157.\"a=b\" TO 'one'",
    "SET r157.\"select\" TO 'one'",
    "SET r157.中文 TO 'one'",
    "SET 中文.r157 TO 'one'",
    "SET r157._a9 TO 'one'",
    "SET r157.a$b TO 'one'",
    "SET r157.a TO 'a=b'",
    "SET SESSION r157.a = 'one'",
    "SET LOCAL r157.a TO 'one'",
    "SET local.extra TO 'one'",
    "SET session.extra TO 'one'",
    "SET role.extra TO 'one'",
    "SET transaction.extra TO 'one'",
    "SET r157.a FROM CURRENT",
    "SET r157.a TO DEFAULT",
    "SET r157.a = 'one';",
    "SET r157.a TO 'one';;",
    "/*lead*/SET/*gap*/r157/*dot*/./*part*/a TO 'one'",
    "SET SESSION session.extra TO 'one'",
    "SET LOCAL local.extra TO 'one'"
]
INVALID = [
    "SET r157. TO 'one'",
    "SET r157.",
    "SET r157..a TO 'one'",
    "SET r157...a TO 'one'",
    "SET r157.1 TO 'one'",
    "SET r157. -1 TO 'one'",
    "SET r157. +a TO 'one'",
    "SET r157.'a' TO 'one'",
    "SET r157.* TO 'one'",
    "SET r157.foo().a TO 'one'",
    "SET r157.a[1] TO 'one'",
    "SET r157.E'a' TO 'one'",
    "SET r157.$$a$$ TO 'one'",
    "SET r157.a 'one'",
    "SET r157.a TO",
    "SET r157.a =",
    "SET r157.a TO;",
    "SET r157.a TO 'one'; SELECT 1",
    "SET r157 . TO 'one'",
    "SET r157.a..b TO 'one'",
    "SET .r157.a TO 'one'",
    "SET 1 . r157 TO 'one'",
    "SET r157.a FROM CURRENT junk",
    "SET r157.a FROM",
    "SET r157.a;"
]


def main():
    review = Review("SET_TARGET_PROTOCOL")

    def describe(sql, state=None):
        packet = review.client.typed(b"P", b"\0"+sql.encode()+b"\0"+struct.pack("!H", 0))
        packet += review.client.typed(b"D", b"S\0")+review.client.typed(b"S")
        review.sock.sendall(packet)
        messages = review.client.read_until_ready(review.sock)
        kinds = [kind for kind, _ in messages]
        states = [review.client.diagnostic_fields(body).get(b"C", b"").decode()
                  for kind, body in messages if kind == b"E"]
        good = messages[-1] == (b"Z", b"I")
        if state:
            good = good and states == [state] and kinds == [b"E", b"Z"]
        else:
            good = good and kinds == [b"1", b"t", b"n", b"Z"] and not states
            good = good and [body for kind, body in messages if kind == b"t"] == [b"\0\0"]
        review.record(("PARSE/DESCRIBE ONLY", sql), good, (states, kinds), state)

    try:
        baseline = review.runner.decode_wire_result(
            review.client.simple_query(review.sock, "SHOW search_path"), include_types=True)
        good = (baseline[1] is None and len(baseline[0]) == 1 and len(baseline[0][0]) == 1
                and baseline[0][0][0] is not None and baseline[4] == "SHOW" and baseline[5] == [25])
        review.record("baseline search_path", good, baseline, "one non-NULL text setting")
        assert len(KEYWORDS) == 494 and len(TYPE_FUNCTION_NAMES) == 23
        for item in KEYWORDS:
            word, category = item.split(":")
            state = "42601" if category == "R" or word in TYPE_FUNCTION_NAMES else None
            describe("SET r157."+word+" TO 'one'", state)
            describe('SET r157."'+word+'" TO \'one\'')
            describe("SET "+word+".r157 TO 'one'", state)
        for sql in VALID:
            describe(sql)
        for sql in INVALID:
            describe(sql, "42601")
        # Every successful Parse above is metadata-only, including LOCAL.
        # It must not run a setting or change the real builtin search path.
        review.query("SHOW search_path", rows=baseline[0], oids=[25], tag="SHOW")
        review.query("SELECT 1 AS v", rows=[["1"]], oids=[23])
        review.complete(1538)
    finally:
        review.close()


if __name__ == "__main__":
    main()
