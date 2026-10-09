"""Bound grouped-column predicates preserve canonical delimited identities."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("having_identity_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    checked = 0

    def check(sql, rows=None, oids=None, state=None):
        nonlocal checked
        checked += 1
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("GROUP_HAVING_IDENTITY", sql, result, flush=True)
        try:
            assert result[1] == state, (sql, result, state)
            if rows is not None:
                assert result[0] == rows and result[4] == "SELECT " + str(len(rows)), (sql, result, rows)
            if oids is not None:
                assert result[5] == oids, (sql, result, oids)
        except AssertionError as error:
            failures.append(error.args)

    try:
        if reference:
            check("BEGIN")
        check('CREATE TABLE having_identity(id INT,"ID" INT,"value key" INT,"x.y" INT,"a""b" INT,"x>y" INT)')
        check("INSERT INTO having_identity VALUES(1,1,1,1,1,1),(2,2,2,2,2,2),(NULL,NULL,NULL,NULL,NULL,NULL)")
        for column in ('id', '"ID"', '"value key"', '"x.y"', '"a""b"', '"x>y"'):
            for qualifier in ('h.', 'public.having_identity.'):
                source = 'having_identity AS h' if qualifier == 'h.' else 'public.having_identity'
                key = qualifier + column
                for operator, right, rows in (
                    ('=', '1', [["1"]]), ('>', '1', [["2"]]),
                    ('<>', '1', [["2"]]), ('=', 'NULL', [])):
                    check('SELECT ' + column + ' FROM ' + source + ' GROUP BY ' + key +
                          ' HAVING ' + key + operator + right + ' ORDER BY ' + column,
                          rows, [23])
        # Existing unquoted controls keep the same canonical column owner.
        check("SELECT id FROM having_identity GROUP BY id HAVING id=1", [["1"]], [23])
        check('SELECT id FROM having_identity AS "m" GROUP BY m.id HAVING m.id=1', [["1"]], [23])
        check('SELECT id,count(*) FROM having_identity AS "M" GROUP BY "M".id HAVING "M".id=1',
              [["1", "1"]], [23,20])
        print("GROUP_HAVING_IDENTITY_COMPLETE", checked, "FAILED", len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
