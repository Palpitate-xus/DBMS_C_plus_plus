#!/usr/bin/env python3
"""R166: finite whole-scalar parameter declarations, not general inference."""
from compat.numeric_type_review import Review
from qualified_scalar_parse_validation_protocol_e2e_test import Wire, identifier
import struct
import uuid

EXPECTED_CONTROLS = 263


class InferenceWire(Wire):
    def inferred(self, statement, sql, oid, declared, resolved):
        messages = self.exchange(self.parse(statement, sql, declared) + self.describe(b"S", statement))
        parameter_body = struct.pack("!H", len(resolved))
        parameter_body += b"".join(struct.pack("!I", n) for n in resolved)
        good = ([kind for kind, _ in messages] == [b"1", b"t", b"T", b"Z"] and
                self.descriptions(messages) == [self.fields(oid)] and
                [body for kind, body in messages if kind == b"t"] == [parameter_body] and
                messages[-1:] == [(b"Z", b"I")])
        self.review.record(("INFERRED PARSE", statement, sql, declared), good,
                           messages, (resolved, self.fields(oid), "no execution"))


def main():
    review = Review("WHOLE_SCALAR_PARAMETER_INFERENCE_PROTOCOL")
    wire = InferenceWire(review)
    schema = identifier("r166_" + uuid.uuid4().hex[:12])
    mix, same, promote, echo, effect = (schema + "." + n for n in ("mix", "same", "promote", "echo", "effect"))
    sequence = schema + ".effects"
    try:
        wire.command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
        wire.command("CREATE FUNCTION " + mix + "(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                     "AS $$SELECT t||':'||n::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + same + "(t TEXT,u TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                     "AS $$SELECT t||':'||u$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + promote + "(n BIGINT) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT "
                     "AS $$SELECT n::text$$", "CREATE FUNCTION")
        wire.command("CREATE FUNCTION " + echo + "(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT "
                     "AS $$SELECT n$$", "CREATE FUNCTION")
        wire.command("CREATE SEQUENCE " + sequence, "CREATE SEQUENCE")
        wire.command("CREATE FUNCTION " + effect + "(n INTEGER) RETURNS INTEGER VOLATILE LANGUAGE plpgsql AS "
                     "$$BEGIN PERFORM nextval('" + sequence + "'); RETURN n; END;$$", "CREATE FUNCTION")

        cases = []
        for declared in ((), (0, 0), (25,), (0, 23), (25, 0), (25, 23)):
            cases.append((mix + "($1,$2)", declared, (25, 23), 25,
                          ((("bind", 7), ["bind:7"]), ((None, -1), [None]))))
        for declared in ((), (0,), (23,)):
            cases.append((mix + "($2,$1)", declared, (23, 25), 25,
                          (((7, "swap"), ["swap:7"]), ((-1, None), [None]))))
        for declared in ((), (0,)):
            cases.append((same + "($1,$1)", declared, (25,), 25,
                          ((("repeat",), ["repeat:repeat"]), ((None,), [None]))))
        for declared, resolved in (((), (20,)), ((23,), (23,))):
            cases.append((promote + "($1)", declared, resolved, 25,
                          (((7,), ["7"]), ((None,), [None]))))
        for declared in ((), (0,)):
            cases.append((echo + "($1)", declared, (23,), 23,
                          (((-7,), [-7]), ((None,), [None]))))
        cases.append(("pg_catalog.sin($1)", (), (701,), 701,
                      (((0.0,), [0.0]), ((None,), [None]))))
        assert len(cases) == 16
        for index, (expression, declared, resolved, oid, values) in enumerate(cases):
            statement = ("r166_case_" + str(index)).encode()
            wire.inferred(statement, "SELECT " + expression + " AS v", oid, declared, resolved)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, oid, resolved, values[0][0], binary_input, binary_output)
                    for iteration, (arguments, expected) in enumerate(values):
                        wire.run(statement, oid, expected, resolved, arguments,
                                 binary_input, binary_output, iteration)
            wire.close(statement)

        valid = "SELECT pg_catalog.sin(0::float8) AS v"
        negatives = ((mix + "($1,$2)", (23, 25), "42883"),
                     (mix + "($1)", (), "42883"),
                     (mix + "($1,$1)", (), "42P08"),
                     (mix + "($1,$3)", (), "42P18"),
                     (mix + "($1,$2)", (25, 20), "42883"))
        for index, (expression, inputs, state) in enumerate(negatives):
            wire.retry_cycle(("r166_bad_" + str(index)).encode(),
                             "SELECT " + expression + " AS v", valid, state, inputs)

        next_value = "SELECT pg_catalog.nextval('" + sequence + "') AS v"
        wire.simple(next_value, [1], 20)
        statement = b"r166_metadata_effect"
        wire.inferred(statement, "SELECT " + effect + "($1) AS v", 23, (), (23,))
        for binary_input in (False, True):
            for binary_output in (False, True):
                wire.bind_describe(statement, 23, (23,), (7,), binary_input, binary_output)
        wire.simple(next_value, [2], 20)
        for binary in (False, True):
            wire.run(statement, 23, [7], (23,), (7,), binary, binary)
        wire.simple(next_value, [5], 20)
        wire.close(statement)
    finally:
        try:
            wire.command("DROP SCHEMA " + schema + " CASCADE", "DROP SCHEMA", notices=True)
        finally:
            review.close()
    # Setup7 + 16*(Parse1+BindDescribe4+Execute8+Close1) + negative5*4
    # + effect/purity11 + cleanup1 =263. Retain every control after failures.
    review.complete(EXPECTED_CONTROLS)


if __name__ == "__main__":
    main()
