#!/usr/bin/env python3
"""Inheritance commands bind complete parent names before changing the catalog."""

import struct

from compat.numeric_type_review import Review


def main():
    review = Review("QUALIFIED_INHERITANCE")
    client, sock = review.client, review.sock

    def command(sql, tag, state=None, parameter_status=None):
        frames = [frame for frame in client.simple_query(sock, sql) if frame[0] != b"N"]
        if state:
            error = client.diagnostic_fields(frames[0][1]) if frames[0][0] == b"E" else {}
            result = [kind for kind, _ in frames], error.get(b"C"), frames[-1]
            expected = [b"E", b"Z"], state.encode(), (b"Z", b"I")
        else:
            result = frames
            expected = [(b"C", tag.encode() + b"\0")]
            if parameter_status is not None:
                expected.append((b"S", parameter_status))
            expected.append((b"Z", b"I"))
        review.record(sql, result == expected, result, expected)

    def rows(source, expected, columns="id,note", oids=(23, 25)):
        review.query(f"SELECT {columns} FROM {source} ORDER BY id LIMIT 32",
                     rows=expected, oids=list(oids))

    try:
        for name in ("inh_a", "inh_b", '"Archive.V1"'):
            command(f"CREATE SCHEMA {name}", "CREATE SCHEMA")
        command("CREATE TABLE inh_a.parent(id int, note text DEFAULT 'a')", "CREATE TABLE")
        command("CREATE TABLE inh_b.parent(id int, note text DEFAULT 'b')", "CREATE TABLE")
        command('CREATE TABLE "Archive.V1"."Parent""Name"(id int, note text)', "CREATE TABLE")
        command("INSERT INTO inh_a.parent(id) VALUES (1)", "INSERT 0 1")
        command("INSERT INTO inh_b.parent(id) VALUES (2)", "INSERT 0 1")
        command('INSERT INTO "Archive.V1"."Parent""Name" VALUES (3,\'quoted parent\')', "INSERT 0 1")
        command("SET search_path = inh_b, public", "SET", parameter_status=b"search_path\0inh_b, public\0")

        command("CREATE TABLE inh_b.child (extra int) INHERITS (Inh_A /* schema */ . ParenT)", "CREATE TABLE")
        command("INSERT INTO inh_b.child(id,extra) VALUES (4,40)", "INSERT 0 1")
        rows("inh_a.parent", [["1", "a"], ["4", "a"]])
        rows("ONLY inh_a.parent", [["1", "a"]])
        rows("inh_b.parent", [["2", "b"]])
        rows("inh_b.child", [["4", "a", "40"]], columns="id,note,extra", oids=(23, 25, 23))

        command("CREATE TABLE inh_b.same_schema () INHERITS (PaReNt)", "CREATE TABLE")
        command("INSERT INTO inh_b.same_schema(id) VALUES (5)", "INSERT 0 1")
        rows("inh_b.parent", [["2", "b"], ["5", "b"]])

        command('CREATE TABLE inh_b.quoted () INHERITS ("Archive.V1" . "Parent""Name")', "CREATE TABLE")
        command("INSERT INTO inh_b.quoted VALUES (6,'quoted child')", "INSERT 0 1")
        rows('"Archive.V1"."Parent""Name"', [["3", "quoted parent"], ["6", "quoted child"]])

        sql = "CREATE TABLE inh_b.shared (note text DEFAULT 'shared') INHERITS (inh_a.parent, inh_b.parent)"
        statement = b"qualified_ddl"
        parse = statement + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0" + statement + b"\0" + struct.pack("!HHH", 0, 0, 0)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"B", bind)
                     + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        frames = [frame for frame in client.read_until_ready(sock) if frame[0] != b"N"]
        expected = [(b"1", b""), (b"2", b""), (b"C", b"CREATE TABLE\0"), (b"Z", b"I")]
        review.record(sql, frames == expected, frames, expected)
        command("INSERT INTO inh_b.shared(id) VALUES (7)", "INSERT 0 1")
        rows("inh_a.parent", [["1", "a"], ["4", "a"], ["7", "shared"]])
        rows("inh_b.parent", [["2", "b"], ["5", "b"], ["7", "shared"]])

        command("CREATE TABLE inh_b.alt(id int,note text)", "CREATE TABLE")
        command("INSERT INTO inh_b.alt VALUES (8,'attached')", "INSERT 0 1")
        command("ALTER TABLE inh_b.alt INHERIT INH_A.parent, ADD COLUMN extra int", "ALTER TABLE")
        rows("inh_a.parent", [["1", "a"], ["4", "a"], ["7", "shared"], ["8", "attached"]])
        rows("inh_b.alt", [["8", "attached", None]], columns="id,note,extra", oids=(23, 25, 23))
        command("ALTER TABLE inh_b.alt NO INHERIT inh_a.parent", "ALTER TABLE")
        rows("inh_a.parent", [["1", "a"], ["4", "a"], ["7", "shared"]])
        rows("inh_b.alt", [["8", "attached"]])

        command("CREATE VIEW inh_a.not_parent AS SELECT id FROM ONLY inh_a.parent", "CREATE VIEW")
        command("CREATE MATERIALIZED VIEW inh_a.not_parent_mv AS SELECT id FROM ONLY inh_a.parent", "SELECT 1")
        for clause, state in (
            ("(inh_a.absent)", "42P01"),
            ("(inh_absent.parent)", "3F000"),
            ("(inh_b.parent, PARENT)", "42P07"),
            ("(inh_a.parent inh_b.parent)", "42601"),
            ("(inh_a.parent,)", "42601"),
            ("()", "42601"),
            ("(inh_a.parent", "42601"),
            ("(inh_a.not_parent)", "42809"),
            ("(inh_a.not_parent_mv)", "42809"),
        ):
            command(f"CREATE TABLE inh_b.bad () INHERITS {clause}", None, state)
        review.query("SELECT id FROM inh_b.bad", state="42P01")
        for clause, state in (
            ("INHERIT inh_a.absent", "42P01"),
            ("NO INHERIT inh_absent.parent", "3F000"),
            ("INHERIT inh_a.parent ignored", "42601"),
            ("NO INHERIT , ADD COLUMN ignored int", "42601"),
        ):
            command(f"ALTER TABLE inh_b.alt {clause}", None, state)
        rows("inh_a.parent", [["1", "a"], ["4", "a"], ["7", "shared"]])
        rows("inh_b.parent", [["2", "b"], ["5", "b"], ["7", "shared"]])
        review.complete(52)
    finally:
        review.close()


if __name__ == "__main__":
    main()
