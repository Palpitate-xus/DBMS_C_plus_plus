#!/usr/bin/env python3
"""Check result descriptions and valid rows before a Simple Query error."""

from struct import pack

from compat import pg_diff_runner as runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql, kinds, tags=(), rows=(), state=None, ready=b"I"):
        frames = client.simple_query(sock, sql)
        values, code, _, _, _ = runner.decode_wire_result(frames)
        actual_tags = tuple(body.rstrip(b"\0").decode() for kind, body in frames if kind == b"C")
        assert (b"".join(kind for kind, _ in frames), actual_tags, values, code, frames[-1]) == (
            kinds, tags, [list(row) for row in rows], state, (b"Z", ready)), (sql, frames)
        return frames

    table = "public.runtime_result_rows"
    count = f'SELECT count(10/divisor) AS "Rows examined" FROM {table}'
    projection = f"WITH input AS (SELECT id,label,divisor FROM {table}) " \
                 "SELECT id,label,CASE WHEN id>0 THEN 10/divisor ELSE id END AS quotient FROM input"
    prefix = (("1", "first", "5"), ("2", None, "2"))

    try:
        query(f"CREATE TABLE {table}(id integer PRIMARY KEY,label varchar(12),divisor integer)",
              b"CZ", ("CREATE TABLE",))
        query(f"INSERT INTO {table} VALUES (1,'first',2),(2,NULL,5),(3,'bad',0),(4,'later',1)",
              b"CZ", ("INSERT 0 4",))

        frames = query(count, b"TEZ", state="22012")
        assert client.row_description_fields(frames) == [(b"Rows examined", 0, 0, 20, 8, -1, 0)], frames
        for sql, state in (
            (f"SELECT count(1/0) FROM {table}", "22012"),
            (f"SELECT count('bad'::integer) FROM {table}", "22P02"),
            (f"SELECT count(*) FILTER (WHERE 1/0>0) FROM {table}", "22012"),
            (f"SELECT count(missing_column) FROM {table}", "42703"),
            ("SELECT count(*) FROM missing_runtime_result_rows", "42P01"),
            (f"SELECT count(*) FROM {table}; garbage", "42601"),
        ):
            query(sql, b"EZ", state=state)

        frames = query(projection, b"TDDEZ", rows=prefix, state="22012")
        expected = [(b"id", 23, 4, -1, 0), (b"label", 1043, -1, 16, 0), (b"quotient", 23, 4, -1, 0)]
        assert [(field[0], *field[3:]) for field in client.row_description_fields(frames)] == expected, frames
        query(projection + " LIMIT 2", b"TDDCZ", ("SELECT 2",), rows=prefix)
        query(projection + " LIMIT 3", b"TDDEZ", rows=prefix, state="22012")
        query(projection + " LIMIT 0", b"TCZ", ("SELECT 0",))

        query(f"INSERT INTO {table} VALUES (9,'rolled back',2); {count}; "
              f"INSERT INTO {table} VALUES (10,'unreached',2)", b"CTEZ", ("INSERT 0 1",), state="22012")
        query(f"SELECT count(*) FROM {table}", b"TDCZ", ("SELECT 1",), (("4",),))
        query("BEGIN", b"CZ", ("BEGIN",), ready=b"T")
        query("SAVEPOINT runtime_failure", b"CZ", ("SAVEPOINT",), ready=b"T")
        query(projection, b"TDDEZ", rows=prefix, state="22012", ready=b"E")
        query(f"SELECT count(*) FROM {table}", b"EZ", state="25P02", ready=b"E")
        query("ROLLBACK TO SAVEPOINT runtime_failure", b"CZ", ("ROLLBACK",), ready=b"T")
        query(f"SELECT count(*) FROM {table}", b"TDCZ", ("SELECT 1",), (("4",),), ready=b"T")
        query("COMMIT", b"CZ", ("COMMIT",))

        typed = client.typed
        sock.sendall(typed(b"P", b"\0" + count.encode() + b"\0" + pack("!H", 0)) +
                     typed(b"B", b"\0\0" + pack("!HHH", 0, 0, 0)) + typed(b"D", b"P\0") +
                     typed(b"E", b"\0" + pack("!I", 0)) + typed(b"S"))
        frames = client.read_until_ready(sock)
        assert b"".join(kind for kind, _ in frames) == b"12TEZ", frames
        assert runner.decode_wire_result(frames)[1] == "22012" and frames[-1] == (b"Z", b"I"), frames
        query(f"SELECT count(*) FROM {table}", b"TDCZ", ("SELECT 1",), (("4",),))
    finally:
        runner.stop_ours(server)
    print("[SIMPLE QUERY RUNTIME RESULT] result description, row prefix and transaction checks passed")


if __name__ == "__main__":
    main()
