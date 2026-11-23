#!/usr/bin/env python3
"""Check TIMETZ SQL precision and the declared scalar binary input format."""

import struct

from compat.numeric_type_review import Review


TIMETZ = 1266
DAY = 86_400_000_000


def main():
    review = Review("TIMETZ_PRECISION")
    client = review.client
    name = b"timetz_precision\0"

    def packet(kind, payload=b""):
        return client.typed(kind, payload)

    def exchange(request):
        review.sock.sendall(request + packet(b"S"))
        return client.read_until_ready(review.sock)

    def check(label, messages, kinds, precision=-1, output=0, value=None, state=None):
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        states = [client.diagnostic_fields(body).get(b"C") for kind, body in messages if kind == b"E"]
        values = client.data_row_values(messages)
        tags = [body for kind, body in messages if kind == b"C"]
        actual = (b"".join(kind for kind, _ in messages), messages[-1], fields, values, states, tags)
        expected = (
            kinds, (b"Z", b"I"),
            [(b"v", 0, 0, TIMETZ, 12, precision, output)] if b"T" in kinds else [],
            [[value]] if b"D" in kinds else [],
            [state.encode()] if state else [],
            [b"SELECT 1\0"] if b"C" in kinds else [],
        )
        review.record(label, actual == expected, actual, expected)

    def simple(expr, precision, value=None, state=None):
        sql = f"SELECT {expr} AS v"
        messages = client.simple_query(review.sock, sql)
        check(sql, messages, b"EZ" if state else b"TDCZ", precision, value=value, state=state)

    def prepare(sql, precision):
        parse = packet(b"P", name + sql.encode() + b"\0" + struct.pack("!HI", 1, TIMETZ))
        messages = exchange(parse + packet(b"D", b"S" + name))
        check("Describe " + sql, messages, b"1tTZ", precision)
        parameter_types = [body for kind, body in messages if kind == b"t"]
        review.record("Declared TIMETZ parameter", parameter_types == [struct.pack("!HI", 1, TIMETZ)],
                      parameter_types, [struct.pack("!HI", 1, TIMETZ)])

    def bind(raw, input_format, output, precision, value=None, state=None):
        inputs = struct.pack("!HHHi", 1, input_format, 1, -1 if raw is None else len(raw))
        formats = struct.pack("!HH", 1, output)
        request = packet(b"B", b"\0" + name + inputs + (raw or b"") + formats)
        request += packet(b"D", b"P\0") + packet(b"E", b"\0" + struct.pack("!I", 0))
        label = (raw, input_format, output, precision)
        check(label, exchange(request), b"EZ" if state else b"2TDCZ",
              precision, output, value, state)

    def close():
        messages = exchange(packet(b"C", b"S" + name))
        expected = [(b"3", b""), (b"Z", b"I")]
        review.record("Close statement", messages == expected, messages, expected)

    try:
        cases = (
            ("'23:59:59.999999+00'::timetz(0)", 0, b"24:00:00+00", None),
            ("timetz(2) '12:32:10.125+0530'", 2, b"12:32:10.13+05:30", None),
            ("'12:32:10.125+503'::time(2) with time zone", 2, b"12:32:10.13+05:03", None),
            ("CAST('00:00:00.1234565-00:30' AS pg_catalog.timetz(6))", 6, b"00:00:00.123456-00:30", None),
            ("'00:00:00.1234575Z'::timetz(6)", 6, b"00:00:00.123458+00", None),
            ("'00:00:00.1234565-00:00:07'::timetz", -1, b"00:00:00.123456-00:00:07", None),
            ("CAST(CAST('23:59:59.984654+08:30' AS timetz(3)) AS timetz(2))", 2, b"23:59:59.99+08:30", None),
            ("'10:20.5+05'::timetz(3)", 3, b"00:10:20.5+05", None),
            ("NULL::timetz(3)", 3, None, None),
            ("'24:00:01+00'::timetz", -1, None, "22008"),
            ("'12:00:00+16'::timetz", -1, None, "22009"),
            ("'bad+05'::timetz", -1, None, "22007"),
        )
        for expr, precision, value, state in cases:
            simple(expr, precision, value, state)

        fractions = (
            (57_000_000, b"57"), (57_000_000, b"57"), (56_990_000, b"56.99"),
            (56_988_000, b"56.988"), (56_987_700, b"56.9877"),
            (56_987_650, b"56.98765"), (56_987_654, b"56.987654"),
        )
        for precision, (micros, seconds) in enumerate(fractions):
            prepare(f"SELECT $1::timetz({precision}) AS v", precision)
            expected_text = b"12:34:" + seconds + b"+08:30:05"
            expected_binary = struct.pack("!qi", 45_240_000_000 + micros, -30_605)
            for output, value in enumerate((expected_text, expected_binary)):
                bind(b"12:34:56.987654+08:30:05", 0, output, precision, value)
                bind(struct.pack("!qi", 45_296_987_654, -30_605), 1, output, precision, value)
                bind(None, 1, output, precision)
            close()

        prepare("SELECT $1 AS v", -1)
        bind(struct.pack("!qi", DAY, 57_599), 1, 0, -1, b"24:00:00-15:59:59")
        bind(struct.pack("!qi", 1, 7), 1, 1, -1, struct.pack("!qi", 1, 7))
        rejected = (
            (b"", "08P01"), (struct.pack("!q", 0), "08P01"),
            (struct.pack("!qi", -1, 0), "22008"), (struct.pack("!qi", DAY + 1, 0), "22008"),
            (struct.pack("!qi", 0, -57_600), "22009"), (struct.pack("!qi", 0, 57_600), "22009"),
            (struct.pack("!qi", 0, 0) + b"\0", "22P03"),
        )
        for raw, state in rejected:
            bind(raw, 1, 1, -1, state=state)
        bind(b"12:00:00+16:00", 0, 0, -1, state="22009")
        bind(b"bad+05", 0, 0, -1, state="22007")
        bind(struct.pack("!qi", 0, -7), 1, 0, -1, b"00:00:00+00:00:07")
        close()
        review.complete(90)
    finally:
        review.close()


if __name__ == "__main__":
    main()
