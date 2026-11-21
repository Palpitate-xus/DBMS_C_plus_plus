#!/usr/bin/env python3
"""Quoted predicate identifiers and literals survive the engine bridge."""

import importlib.util
import json
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "quoted_predicate_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def extended(sql):
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
        execute = b"\0" + struct.pack("!I", 0)
        server["sock"].sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                               client.typed(b"E", execute) + client.typed(b"S"))
        return client.read_until_ready(server["sock"])

    def query(sql, expected=None, tag=None, ready=b"I", transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, command_tag = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert command_tag == tag, (sql, command_tag, tag)
        return rows

    def error(sql, expected, ready=b"E"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])

    try:
        query('CREATE TABLE quoted_predicate("child id" INT PRIMARY KEY,payload TEXT,"nullable text" TEXT,"a""b" TEXT,"key.name" INT);')
        query("INSERT INTO quoted_predicate VALUES(1,'original',NULL,'has space',7),(2,'other','','plain',8);")
        query('UPDATE quoted_predicate SET payload=\'changed\' WHERE "child id"=1 RETURNING "child id",payload;', [["1", "changed"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'empty\' WHERE "nullable text"=\'\' RETURNING "child id";', [["2"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'null\' WHERE "nullable text" IS NULL RETURNING "child id";', [["1"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'like\' WHERE "a""b" LIKE \'has space%\' RETURNING "child id";', [["1"]], "UPDATE 1")
        query('DELETE FROM quoted_predicate WHERE "key.name"=7 AND "child id"<>2 RETURNING "child id",payload;', [["1", "like"]], "DELETE 1", transport=extended)
        query('DELETE FROM quoted_predicate WHERE "nullable text" IS NULL;', tag="DELETE 0")
        query('DELETE FROM quoted_predicate WHERE "nullable text" IS NOT NULL RETURNING "child id";', [["2"]], "DELETE 1")
        query('SELECT "child id" FROM quoted_predicate;', [])
        query('CREATE TABLE quoted_parent("key id" INT PRIMARY KEY);')
        query('INSERT INTO quoted_parent VALUES(1);')
        query('CREATE TABLE quoted_child("child id" INT PRIMARY KEY,"parent id" INT REFERENCES quoted_parent("key id"));')
        query('INSERT INTO quoted_child VALUES(1,1);')
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT quoted_sp;", ready=b"T")
        error('UPDATE quoted_child SET "parent id"=999 WHERE "child id"=1 RETURNING "child id";', "23503")
        error('SELECT "child id" FROM quoted_child;', "25P02")
        query("ROLLBACK TO SAVEPOINT quoted_sp;", ready=b"T")
        query('SELECT "child id","parent id" FROM quoted_child;', [["1", "1"]], ready=b"T")
        query("COMMIT;")
        query('DELETE FROM quoted_child WHERE "child id">=1 AND "child id"<=1 RETURNING "child id";', [["1"]], "DELETE 1")

        query('CREATE TABLE quoted_select_predicate(rid INT,id BIGINT,"7" BIGINT,"-7" BIGINT,"q""r" BIGINT);')
        error('SELECT rid FROM quoted_select_predicate WHERE id="missing";', "42703", ready=b"I")
        query('INSERT INTO quoted_select_predicate VALUES '
              '(1,7,99,7,0),(2,17,7,17,0),(3,42,42,0,42),'
              '(4,9007199254740992,9007199254740993,0,1),'
              '(5,9007199254740993,9007199254740993,0,1),'
              '(6,NULL,7,0,0),(7,-7,-7,88,NULL),(8,-9223372036854775808,NULL,0,0);')
        for indexed in (False, True):
            if indexed:
                query('CREATE INDEX quoted_select_id ON quoted_select_predicate(id);')
            query('SELECT rid FROM quoted_select_predicate WHERE id=7;', [["1"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id=+7 ORDER BY rid;', [["1"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id=-7;', [["7"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id=-9223372036854775808;', [["8"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id="7" ORDER BY rid;',
                  [["3"], ["5"], ["7"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id<"7" ORDER BY rid;',
                  [["1"], ["4"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id="q""r";', [["3"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id="7" OR id=7 ORDER BY rid;',
                  [["1"], ["3"], ["5"], ["7"]])
            query('SELECT rid FROM quoted_select_predicate WHERE id=7 LIMIT 1;', [["1"]])
            column_filter = 'SELECT rid FROM quoted_select_predicate WHERE id="7";'
            query('EXPLAIN ' + column_filter)
            explained = query('EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) ' + column_filter)
            document = json.loads("\n".join(row[0] for row in explained))
            assert document["actualRows"] == 3, document

        query('CREATE TABLE select_column_pair(rid INT,lhs BIGINT,rhs BIGINT,a TEXT,b TEXT);')
        error('SELECT rid FROM select_column_pair WHERE lhs=missing;', "42703", ready=b"I")
        query("INSERT INTO select_column_pair VALUES "
              "(1,9007199254740992,9007199254740993,'01','1'),"
              "(2,9007199254740993,9007199254740993,'0.50','0.5'),"
              "(3,7,7,'same','same'),(4,17,7,'a','b'),"
              "(5,NULL,7,NULL,'same'),(6,7,NULL,'z',NULL);")
        for indexed in (False, True):
            if indexed:
                query('CREATE INDEX select_column_pair_lhs ON select_column_pair(lhs);')
            query('SELECT rid FROM select_column_pair WHERE lhs=rhs ORDER BY rid;', [["2"], ["3"]])
            query('SELECT rid FROM select_column_pair WHERE lhs<>rhs ORDER BY rid;', [["1"], ["4"]])
            query('SELECT rid FROM select_column_pair WHERE lhs<rhs ORDER BY rid;', [["1"]])
            query('SELECT rid FROM select_column_pair AS p WHERE p.rhs=p.lhs ORDER BY rid;',
                  [["2"], ["3"]])
            query('SELECT rid FROM select_column_pair WHERE a=b ORDER BY rid;', [["3"]])
            query('SELECT rid FROM select_column_pair WHERE a<b ORDER BY rid;', [["1"], ["4"]])
            query('SELECT rid FROM select_column_pair WHERE lhs=rhs OR lhs=17 ORDER BY rid;',
                  [["2"], ["3"], ["4"]])
            query('SELECT rid FROM select_column_pair WHERE lhs=7 AND lhs=rhs;', [["3"]])
            query('SELECT rid FROM select_column_pair WHERE lhs=rhs ORDER BY rid LIMIT 1;', [["2"]])
            explained = query('EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) '
                              'SELECT rid FROM select_column_pair WHERE lhs=rhs;')
            document = json.loads("\n".join(row[0] for row in explained))
            assert document["actualRows"] == 2, document

        query('CREATE TABLE select_mixed_widths(rid INT,"small(width" SMALLINT, mid INT, wide BIGINT, '
              'x TEXT COLLATE "C", y TEXT COLLATE "C");')
        query("INSERT INTO select_mixed_widths VALUES "
              "(1,-32768,-32768,-32768,'',''),(2,-32768,-32767,-32768,' spaced','spaced'),"
              "(3,32767,2147483647,9223372036854775807,'A','a'),"
              "(4,0,0,-9223372036854775808,NULL,''),(5,1,1,1,'',NULL);")
        query('SELECT rid FROM select_mixed_widths WHERE "small(width"=wide ORDER BY rid;',
              [["1"], ["2"], ["5"]])
        query('SELECT rid FROM select_mixed_widths WHERE mid>wide ORDER BY rid;', [["2"], ["4"]])
        query('SELECT rid FROM select_mixed_widths WHERE x=y;', [["1"]])
        query('SELECT rid FROM select_mixed_widths WHERE x<y ORDER BY rid;', [["2"], ["3"]])
        print("[QUOTED COLUMN PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
