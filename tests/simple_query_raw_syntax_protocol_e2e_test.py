#!/usr/bin/env python3
"""R169 Simple Query raw-proof ordering; shared PG18.6 oracle.

Every case keeps all observations after SQL errors. UUID-owned objects only.
No metadata binding is done across statements; runtime/analysis errors must
still show a reached prefix CommandComplete. This does not assert that the
old failing syntax batches persisted DDL: their existing implicit batch owner
already rolls them back. The new difference is no prefix execution/messages.
"""
from compat.numeric_type_review import Review
import struct
import uuid

EXPECTED_CONTROLS = 104


class RawReview(Review):
    def complete(self):
        if self.controls != EXPECTED_CONTROLS:
            self.failures.append(("INVENTORY", self.controls, EXPECTED_CONTROLS))
        print(self.label + "_COMPLETE", self.controls, "FAILURES", len(self.failures), flush=True)
        assert not self.failures, self.failures


def data_rows(messages):
    result = []
    for kind, body in messages:
        if kind != b"D":
            continue
        assert len(body) >= 2
        count = struct.unpack("!H", body[:2])[0]
        at, row = 2, []
        for _ in range(count):
            assert at + 4 <= len(body)
            length = struct.unpack("!i", body[at:at + 4])[0]
            at += 4
            if length == -1:
                row.append(None)
            else:
                assert length >= 0 and at + length <= len(body)
                row.append(body[at:at + length])
                at += length
        assert at == len(body)
        result.append(row)
    return result


def main():
    review = RawReview("SIMPLE_QUERY_RAW_SYNTAX_PROTOCOL")
    client, sock = review.client, review.sock
    token = "r169_" + uuid.uuid4().hex[:12]
    schema = token
    pair, moved = token + "_pair", token + "_moved"
    print("R169_RUN_TOKEN", token, flush=True)

    def check(sql, kinds, tags=(), rows=(), oids=(), state=None, ready=b"I", notices=False, status=None):
        messages = client.simple_query(sock, sql)
        valid = True
        try:
            diagnostics = [client.diagnostic_fields(body) for kind, body in messages if kind == b"N"]
            valid = not diagnostics or (notices and all(item.get(b"S") == b"NOTICE" and item.get(b"C") == b"00000" for item in diagnostics))
            statuses = [body for kind, body in messages if kind == b"S"]
            valid = valid and (statuses == [] or (status is not None and statuses == [status] and messages[-2][0] == b"S"))
            visible = [(kind, body) for kind, body in messages if not (notices and kind == b"N") and not (status is not None and kind == b"S")]
            errors = [client.diagnostic_fields(body).get(b"C", b"").decode() for kind, body in visible if kind == b"E"]
            descriptions = [client.row_description_fields([item]) for item in visible if item[0] == b"T"]
            expected_descriptions = [[(b"v", oid, {20: 8, 23: 4, 25: -1}[oid], -1, 0)] for oid in oids]
            actual_descriptions = [[(field[0], field[3], field[4], field[5], field[6]) for field in fields] for fields in descriptions]
            expected_rows = [[None if value is None else str(value).encode("utf-8")] for value in rows]
            valid = (valid and [kind.decode("ascii") for kind, _ in visible] == list(kinds) and
                     [body for kind, body in visible if kind == b"C"] == [tag.encode() + bytes([0]) for tag in tags] and
                     errors == ([] if state is None else [state]) and data_rows(visible) == expected_rows and
                     actual_descriptions == expected_descriptions and visible[-1:] == [(b"Z", ready)])
        except Exception:
            valid = False
        review.record(sql, valid, messages, (kinds, tags, rows, oids, state, ready))

    def command(sql, tag, ready=b"I", notices=False, status=None):
        check(sql, "CZ", (tag,), ready=ready, notices=notices, status=status)

    def error(sql, state="42601", ready=b"I"):
        check(sql, "EZ", state=state, ready=ready)

    try:
        command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        # Original R167 bad batches retained, expanded only by raw token kind.
        for cascade in (False, True):
            for index, trailer in enumerate(("garbage", "123", "'stray'", "$$stray$$", ")", "(123)")):
                domain = schema + ".guard_" + str(int(cascade)) + "_" + str(index)
                command("CREATE DOMAIN " + domain + " AS INTEGER", "CREATE DOMAIN")
                error("DROP DOMAIN " + domain + (" CASCADE" if cascade else "") + "; " + trailer)
                command("DROP DOMAIN " + domain, "DROP DOMAIN")
        # Whole-message lexical errors must use the same boundary.
        for index, trailer in enumerate(("SELECT '", 'SELECT "', "/*", "SELECT $x$unterminated", "SELECT 1 /* unmatched", "SELECT E'abc\\'")):
            domain = schema + ".lex_" + str(index)
            command("CREATE DOMAIN " + domain + " AS INTEGER", "CREATE DOMAIN")
            error("DROP DOMAIN " + domain + "; " + trailer)
            command("DROP DOMAIN " + domain, "DROP DOMAIN")

        # Nontransactional sequence observation proves no prefix evaluation.
        sequence = schema + ".effect_sequence"
        command("CREATE SEQUENCE " + sequence, "CREATE SEQUENCE")
        error("SELECT nextval('" + sequence + "') AS v; garbage")
        check("SELECT nextval('" + sequence + "') AS v", "TDCZ", ("SELECT 1",), (1,), (20,))
        error("SELECT nextval('" + sequence + "') AS v; DROP DOMAIN")
        check("SELECT currval('" + sequence + "') AS v", "TDCZ", ("SELECT 1",), (1,), (20,))
        command("DROP SEQUENCE " + sequence, "DROP SEQUENCE")

        # Later valid statements may reference objects created in the batch.
        table = schema + ".future"
        check("CREATE TABLE " + table + "(id INTEGER); INSERT INTO " + table + " VALUES(7); SELECT id AS v FROM " + table,
              "CCTDCZ", ("CREATE TABLE", "INSERT 0 1", "SELECT 1"), (7,), (23,))
        function = schema + ".future_function"
        check("CREATE FUNCTION " + function + "() RETURNS INTEGER LANGUAGE plpgsql AS $body$BEGIN RETURN 9; END;$body$; SELECT " + function + "() AS v",
              "CTDCZ", ("CREATE FUNCTION", "SELECT 1"), (9,), (23,))
        check("CREATE TYPE public." + pair + " AS(v INTEGER); ALTER TYPE " + pair +
              " ADD ATTRIBUTE extra TEXT; ALTER TYPE " + pair + " RENAME TO " + moved + "; DROP TYPE public." + moved,
              "CCCCZ", ("CREATE TYPE", "ALTER TYPE", "ALTER TYPE", "DROP TYPE"))
        check("SET search_path=public,pg_catalog; SELECT 1 AS v", "CTDCZ", ("SET", "SELECT 1"), (1,), (23,),
              status=b"search_path" + bytes([0]) + b"public, pg_catalog" + bytes([0]))
        check("SELECT ';' AS v; SELECT $$dollar;value$$ AS v", "TDCTDCZ", ("SELECT 1", "SELECT 1"), (";", "dollar;value"), (25, 25))
        check("/* outer /* nested ; */ comment */ SELECT 3 AS v; -- ignored ;\nSELECT 4 AS v", "TDCTDCZ",
              ("SELECT 1", "SELECT 1"), (3, 4), (23, 23))
        check("SELECT 'quote'';value' AS v; SELECT $tag$-- ; /* literal */$tag$ AS v", "TDCTDCZ",
              ("SELECT 1", "SELECT 1"), ("quote';value", "-- ; /* literal */"), (25, 25))
        check("; -- comment ;\n; /* outer /* nested */ comment */ ;", "IZ")
        check("; SELECT 5 AS v; ; /* no command */ ;", "TDCZ", ("SELECT 1",), (5,), (23,))

        # Analysis and runtime failures stay reached, with the prior C visible.
        check("SELECT 11 AS v; WITH x AS(SELECT 1),x AS(SELECT 2) SELECT * FROM x", "TDCEZ",
              ("SELECT 1",), (11,), (23,), state="42712")
        check("SELECT 12 AS v; SELECT 1/0 AS v", "TDCEZ", ("SELECT 1",), (12,), (23,), state="22012")
        check("SELECT 13 AS v; SELECT missing FROM " + schema + ".absent", "TDCEZ",
              ("SELECT 1",), (13,), (23,), state="42P01")
        # SQLSTATE 42601 alone does not establish raw-parser ownership.
        # These domain constraints are accepted by PG raw ColQualList and
        # rejected by DefineDomain only after the first SELECT completed.
        for index, constraints in enumerate(("NULL NOT NULL", "NOT NULL NULL", "UNIQUE")):
            domain = schema + ".reached_conflict_" + str(index)
            check("SELECT " + str(14 + index) + " AS v; CREATE DOMAIN " + domain + " AS INTEGER " + constraints,
                  "TDCEZ", ("SELECT 1",), (14 + index,), (23,), state="42601")
            command("DROP DOMAIN IF EXISTS " + domain, "DROP DOMAIN", notices=True)

        # Existing I/T/E transitions and recovery remain the owner.
        domain = schema + ".transaction_guard"
        command("BEGIN", "BEGIN", ready=b"T")
        command("CREATE DOMAIN " + domain + " AS INTEGER", "CREATE DOMAIN", ready=b"T")
        error("DROP DOMAIN " + domain + "; garbage", ready=b"E")
        error("SELECT 1 AS v", "25P02", ready=b"E")
        command("ROLLBACK", "ROLLBACK")
        command("CREATE DOMAIN " + domain + " AS INTEGER", "CREATE DOMAIN")
        command("DROP DOMAIN " + domain, "DROP DOMAIN")
        command("BEGIN", "BEGIN", ready=b"T")
        error("SELECT 1/0 AS v", "22012", ready=b"E")
        error("SELECT 1 AS v; 123", ready=b"E")
        error("SELECT 1 AS v", "25P02", ready=b"E")
        command("ROLLBACK", "ROLLBACK")
        command("BEGIN", "BEGIN", ready=b"T")
        check("SELECT 1 AS v; SELECT 2 AS v", "TDCTDCZ", ("SELECT 1", "SELECT 1"), (1, 2), (23, 23), ready=b"T")
        command("ROLLBACK", "ROLLBACK")
        error("BEGIN; SELECT 1 AS v; garbage; ROLLBACK")
        check("SELECT 1 AS v", "TDCZ", ("SELECT 1",), (1,), (23,))
        check("BEGIN; SELECT 1 AS v; SELECT 1/0 AS v; COMMIT", "CTDCEZ",
              ("BEGIN", "SELECT 1"), (1,), (23,), state="22012", ready=b"E")
        command("ROLLBACK", "ROLLBACK")
        command("BEGIN", "BEGIN", ready=b"T")
        error("COMMIT; garbage", ready=b"E")
        command("ROLLBACK", "ROLLBACK")
    finally:
        try:
            command("DROP TYPE IF EXISTS public." + pair, "DROP TYPE", notices=True)
        finally:
            try:
                command("DROP TYPE IF EXISTS public." + moved, "DROP TYPE", notices=True)
            finally:
                try:
                    command("DROP SCHEMA IF EXISTS " + schema + " CASCADE", "DROP SCHEMA", notices=True)
                finally:
                    review.close()
    # Schema setup/cleanup2 + public type cleanup2 + original/raw36 + lexical18
    # + effect6 + valid/Main/trivia9 + analysis/runtime3 + reached-domain6
    # + transaction22 =104. Original 96 observations are all retained.
    review.complete()


if __name__ == "__main__":
    main()
