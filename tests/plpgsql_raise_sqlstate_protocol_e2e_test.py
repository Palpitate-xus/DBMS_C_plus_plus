#!/usr/bin/env python3
"""RAISE preserves its SQLSTATE and enclosing statement rollback."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('raise_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client(); server = runner.start_ours(client)
    def query(sql):
        messages = client.simple_query(server['sock'], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert messages[-1] == (b'Z', b'I'), (sql, messages[-1])
        return result
    def ok(sql, rows=None):
        result = query(sql)
        assert result[1] is None, (sql, result)
        if rows is not None: assert result[0] == rows, (sql, result)
        return result
    try:
        ok('CREATE TABLE raise_sink(id INT);')
        ok('CREATE SEQUENCE raise_seq START 1;')
        cases = (
            ("RAISE EXCEPTION 'late failure'", 'P0001', 'late failure'),
            ("RAISE 'default failure'", 'P0001', 'default failure'),
            ("RAISE 'custom' USING ERRCODE=code", '22012', 'custom'),
            ("RAISE 'named' USING ERRCODE='unique_violation'", '23505', 'named'),
            ("RAISE 'numeric' USING ERRCODE=23505", '23505', 'numeric'),
            ("RAISE 'zero' USING ERRCODE='00000'", 'P0001', 'zero'),
            ("RAISE SQLSTATE 'P1234'", 'P1234', 'P1234'),
            ("RAISE division_by_zero", '22012', 'division_by_zero'),
            ("RAISE USING MESSAGE='from option',ERRCODE='22023'", '22023', 'from option'),
            ("RAISE '%/%%/%/%',NULL,'null','O''Brien'", 'P0001', "<NULL>/%/null/O'Brien"),
            ("RAISE 'x' USING ERRCODE=NULL", '22004', 'cannot be null'),
            ("RAISE 'x' USING ERRCODE='p0001'", '42704', 'unrecognized'),
            ("RAISE 'x' USING ERRCODE='UNIQUE_VIOLATION'", '42704', 'unrecognized'),
            ("RAISE 'x' USING ERRCODE='23505',ERRCODE='22012'", '42601', 'already specified'),
            ("RAISE 'x' USING MESSAGE='other'", '42601', 'already specified'),
            ("RAISE '%',1/0", '22012', ''),
            ("RAISE 'x' USING ERRCODE=CAST('bad' AS INT)", '22P02', ''),
            ('RAISE', '0Z002', 'exception handler'),
        )
        for number, (statement, state, message) in enumerate(cases):
            function = f'raise_case_{number}'
            ok(f"CREATE FUNCTION {function}() RETURNS INT LANGUAGE plpgsql AS $$ "
               f"DECLARE code TEXT:='22012'; BEGIN INSERT INTO raise_sink VALUES({number}); "
               f"PERFORM nextval('raise_seq'); {statement}; RETURN 1; END; $$;")
            result = query(f'SELECT {function}();')
            assert result[1] == state and result[0] == [] and result[4] is None, (number, result)
            assert message in result[2], (number, result)
            ok('SELECT id FROM raise_sink;', [])
        # RAISE args/options are checked during body parsing before reached
        # SQL, so even a nontransactional sequence must remain uncalled.
        ok('CREATE SEQUENCE raise_static_seq START 1;')
        for number, statement in enumerate(("RAISE '%'", "RAISE 'none',1",
                                            "RAISE nonexistent_condition", "RAISE SQLSTATE 'p0001'")):
            ok(f"CREATE FUNCTION raise_static_{number}() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN "
               f"PERFORM nextval('raise_static_seq'); {statement}; RETURN 1; END; $$;")
            result = query(f'SELECT raise_static_{number}();')
            expected = '42704' if number == 2 else '42601'
            assert result[1] == expected, (number, result)
        assert query("SELECT currval('raise_static_seq');")[1] == '55000'
        ok("SELECT nextval('raise_static_seq');", [['1']])
        ok("CREATE FUNCTION raise_nonfatal() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN "
           "RAISE NOTICE 'notice %',1; RAISE WARNING USING MESSAGE='warning'; RETURN 7; END; $$;")
        ok('SELECT raise_nonfatal();', [['7']])
        ok("SELECT currval('raise_seq');", [[str(len(cases))]])
        print('[PLPGSQL RAISE SQLSTATE PROTOCOL] passed')
    finally:
        runner.stop_ours(server)


if __name__ == '__main__': main()
