"""SQL-value grammar carries real static types, labels and reached context."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path
from session_setting_functions_protocol_e2e_test import Wire


def check_temporary_schema(client, sock):
    wire = Wire(client, sock)
    missing = "SELECT 'pg_temp.no_such_table'::text::regclass"
    wire.command("SET search_path=public,pg_temp", "SET")
    wire.query("SELECT current_schema AS n", rows=[("public",)], fields=[("n", 19)])
    wire.query("SELECT pg_my_temp_schema() AS oid", rows=[("0",)], fields=[("oid", 26)])
    wire.query(missing, state="3F000", tag=None)
    wire.command("CREATE TABLE public.current_schema_error_probe(i integer)", "CREATE TABLE")
    wire.command("INSERT INTO public.current_schema_error_probe VALUES(0)", "INSERT 0 1")
    try:
        wire.command("SET search_path=missing_schema,pg_temp,public", "SET")
        sql = "SELECT current_schema AS n"
        sync = wire.packet(b"S")
        frames = wire.exchange(wire.parse(sql) + wire.packet(b"D", b"S\0") + sync)
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert [field[3] for field in client.row_description_fields(frames)] == [19], frames
        wire.query(missing, state="3F000", tag=None)
        wire.query("SELECT CASE WHEN false THEN current_schema ELSE 'kept'::name END AS n",
                   rows=[("kept",)], fields=[("n", 19)])
        wire.query(missing, state="3F000", tag=None)
        wire.query("SELECT CASE WHEN current_schema::text LIKE 'pg_temp_%' THEN 1/i ELSE i END "
                   "FROM public.current_schema_error_probe",
                   state="22012", tag=None)
        wire.query(missing, state="3F000", tag=None)

        def initialize(mode, spelling, ready=b"I"):
            sql = f"SELECT {spelling} AS n"
            frames = mode.send_statements(sql)
            rows = client.data_row_values(frames)
            assert len(rows) == 1 and rows[0][0], (sql, frames)
            value = rows[0][0].decode()
            assert value.startswith("pg_temp_"), (sql, value)
            mode.check(sql, frames, rows=[(value,)], fields=[("n", 19)], ready=ready)
            mode.query(f"SELECT nspname FROM pg_namespace WHERE nspname='{value}'",
                       rows=[(value,)], fields=[("nspname", 19)], ready=ready)
            mode.query("SELECT oid=pg_my_temp_schema() AS same FROM pg_namespace "
                       f"WHERE nspname='{value}'",
                       rows=[("t",)], fields=[("same", 16)], ready=ready)
            return value

        def removed(name, ready=b"I"):
            wire.query(f"SELECT nspname FROM pg_namespace WHERE nspname='{name}'",
                       rows=[], fields=[("nspname", 19)], tag="SELECT 0", ready=ready)
            wire.query("SELECT pg_my_temp_schema() AS oid",
                       rows=[("0",)], fields=[("oid", 26)], ready=ready)

        wire.command("BEGIN READ ONLY", "BEGIN", ready=b"T")
        name = initialize(wire, "current_schema", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        removed(name)
        wire.query(missing, state="3F000", tag=None)
        wire.command("BEGIN", "BEGIN", ready=b"T")
        name = initialize(wire, "current_schema", ready=b"T")
        wire.command("CREATE TABLE public.current_schema_created_in_txn(id int)", "CREATE TABLE", ready=b"T")
        wire.command("INSERT INTO public.current_schema_created_in_txn VALUES(5)", "INSERT 0 1", ready=b"T")
        wire.command("ROLLBACK", "ROLLBACK")
        removed(name)
        wire.query("SELECT * FROM public.current_schema_created_in_txn", state="42P01", tag=None)
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("SAVEPOINT before_temp", "SAVEPOINT", ready=b"T")
        extended = Wire(client, sock, extended=True)
        name = initialize(extended, "current_schema()", ready=b"T")
        wire.command("CREATE TEMP TABLE pg_temp.current_schema_temp_in_savepoint(id int)",
                     "CREATE TABLE", ready=b"T")
        wire.command("INSERT INTO pg_temp.current_schema_temp_in_savepoint VALUES(7)", "INSERT 0 1", ready=b"T")
        wire.command("ROLLBACK TO before_temp", "ROLLBACK", ready=b"T")
        removed(name, ready=b"T")
        wire.query(missing, state="3F000", tag=None, ready=b"E")
        wire.command("ROLLBACK", "ROLLBACK")
        wire.query(missing, state="3F000", tag=None)
        initialize(extended, "pg_catalog.current_schema()")
        wire.command("DISCARD TEMP", "DISCARD TEMP")
        wire.query(missing, state="42P01", tag=None)
    except Exception:
        wire.send_statements("ROLLBACK")
        wire.send_statements("DROP TABLE public.current_schema_error_probe")
        raise
    else:
        wire.command("DROP TABLE public.current_schema_error_probe", "DROP TABLE")


def check_temp_inspection(client, sock, connect):
    owner = Wire(client, sock)
    mine = "SELECT pg_catalog.pg_my_temp_schema() AS n"
    other = "SELECT pg_catalog.pg_is_other_temp_schema({}) AS v"

    def schema_oid(wire):
        frames = wire.send_statements(mine)
        rows = client.data_row_values(frames)
        assert len(rows) == 1 and rows[0][0], frames
        oid = int(rows[0][0])
        wire.check(mine, frames, rows=[(str(oid),)], fields=[("n", 26)])
        return oid

    owner_oid = schema_oid(owner)
    assert owner_oid > 0
    for extended in (False, True):
        wire = Wire(client, sock, extended)
        for argument, expected in ((str(owner_oid), "f"), ("11", "f"), ("0", "f"),
                                   ("'4294967295'", "f"), ("NULL", None)):
            wire.query(other.format(argument), rows=[(expected,)], fields=[("v", 16)])
        wire.query(other.format("'not-an-oid'"), state="22P02", tag=None)
        wire.query(other.format("'11'::text"), state="42883", tag=None)
        wire.query("SELECT pg_my_temp_schema(0)", state="42883", tag=None)
    frames = owner.exchange(owner.parse(mine) + owner.bind(result_format=1) +
                            owner.describe_portal() + owner.execute() + owner.packet(b"S"))
    assert client.data_row_values(frames) == [[struct.pack("!I", owner_oid)]], frames
    assert [(field[3], field[-1]) for field in client.row_description_fields(frames)] == [(26, 1)], frames

    with connect() as second:
        peer = Wire(client, second)
        assert schema_oid(peer) == 0
        peer.query(other.format(owner_oid), rows=[("t",)], fields=[("v", 16)])
        peer.command("CREATE TEMP TABLE temp_inspection_peer(k integer)", "CREATE TABLE")
        peer_oid = schema_oid(peer)
        assert peer_oid > 0 and peer_oid != owner_oid
        peer.command("DROP TABLE pg_temp.temp_inspection_peer", "DROP TABLE")
        assert schema_oid(peer) == peer_oid
        peer.query(other.format(peer_oid), rows=[("f",)], fields=[("v", 16)])
        for extended in (False, True):
            wire = Wire(client, sock, extended)
            for oid, expected in ((owner_oid, "f"), (peer_oid, "t")):
                wire.query(other.format("n.oid") + f" FROM pg_catalog.pg_namespace n WHERE n.oid='{oid}'::oid",
                           rows=[(expected,)], fields=[("v", 16)])
            wire.query("SELECT n.oid FROM pg_catalog.pg_namespace n "
                       f"WHERE n.oid='{peer_oid}'::oid AND pg_catalog.pg_is_other_temp_schema(n.oid)",
                       rows=[(str(peer_oid),)], fields=[("oid", 26)])

        owner.command("CREATE TEMP TABLE temp_inspection_input(oid oid, nspname text)", "CREATE TABLE")
        owner.command(f"INSERT INTO pg_temp.temp_inspection_input VALUES ('{peer_oid}'::oid, 'public')", "INSERT 0 1")
        owner.query(other.format("input.oid") + " FROM pg_temp.temp_inspection_input input",
                    rows=[("t",)], fields=[("v", 16)])
        owner.command("DROP TABLE pg_temp.temp_inspection_input", "DROP TABLE")

        owner.command("CREATE SCHEMA temp_inspection_override", "CREATE SCHEMA")
        try:
            owner.command("CREATE FUNCTION temp_inspection_override.pg_is_other_temp_schema(oid) "
                          "RETURNS boolean LANGUAGE SQL AS $$ SELECT false $$", "CREATE FUNCTION")
            owner.command("SET search_path=temp_inspection_override,pg_catalog,public", "SET")
            owner.query("SELECT pg_is_other_temp_schema(n.oid) AS v "
                        f"FROM pg_catalog.pg_namespace n WHERE n.oid='{peer_oid}'::oid",
                        rows=[("f",)], fields=[("v", 16)])
        finally:
            owner.command("SET search_path=pg_temp,public", "SET")
            owner.command("DROP FUNCTION temp_inspection_override.pg_is_other_temp_schema(oid)", "DROP FUNCTION")
            owner.command("DROP SCHEMA temp_inspection_override", "DROP SCHEMA")

        sql = other.format("$1")
        statement = b"temp_schema_owner_probe"
        frames = owner.exchange(owner.parse(sql, statement) + owner.packet(b"D", b"S" + statement + b"\0")
                                + owner.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"1tTZ", frames
        assert frames[1][1] == struct.pack("!HI", 1, 26), frames
        for oid, expected in ((peer_oid, b"\x01"), (owner_oid, b"\x00"), (None, None)):
            arg = None if oid is None else str(oid).encode()
            frames = owner.exchange(owner.bind(statement, (arg,), result_format=1) +
                                    owner.describe_portal() + owner.execute() + owner.packet(b"S"))
            assert client.data_row_values(frames) == [[expected]], frames
            assert [(field[3], field[-1]) for field in client.row_description_fields(frames)] == [(16, 1)], frames


def check_temp_catalog_change(client, sock):
    wire = Wire(client, sock)
    name = "pg_temp_catalog_move_probe"
    wire.command(f'CREATE SCHEMA "{name}"', "CREATE SCHEMA")
    frames = wire.send_statements(f"SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='{name}'")
    oid = int(client.data_row_values(frames)[0][0])
    wire.command("CREATE FUNCTION public.catalog_move_probe() RETURNS boolean VOLATILE LANGUAGE plpgsql AS $$ "
                 f'BEGIN DROP SCHEMA "{name}"; RETURN true; END; $$', "CREATE FUNCTION")
    try:
        wire.query("SELECT CASE WHEN pg_catalog.pg_is_other_temp_schema(n.oid) THEN "
                   "CASE WHEN public.catalog_move_probe() THEN pg_catalog.pg_is_other_temp_schema(n.oid) "
                   f"ELSE true END ELSE true END AS v FROM pg_catalog.pg_namespace n WHERE n.oid='{oid}'::oid",
                   rows=[("f",)], fields=[("v", 16)])
    finally:
        wire.command("DROP FUNCTION public.catalog_move_probe()", "DROP FUNCTION")


def check_duplicate_namespace_oid(client, server):
    wire = Wire(client, server["sock"])
    ordinary = "duplicate_schema_probe"
    duplicate = "pg_temp_duplicate_oid_fixture"
    wire.command(f"CREATE SCHEMA {ordinary}", "CREATE SCHEMA")
    frames = wire.send_statements(f"SELECT oid FROM pg_namespace WHERE nspname='{ordinary}'")
    oid = int(client.data_row_values(frames)[0][0])
    catalog = Path(server["dir"]) / "info" / "pg_catalog" / "pg_namespace.cat"
    original = catalog.read_bytes()
    fixture = catalog.with_name("namespace-test-fixture.cat")
    try:
        # The private fixture emulates an older image with two physical rows
        # for one OID. Lookup by OID picks the last registered row.
        fixture.write_bytes(original + f'{oid},"{duplicate}",10\n'.encode())
        fixture.replace(catalog)
        wire.query(f"SELECT pg_catalog.pg_is_other_temp_schema('{oid}'::oid) AS v",
                   rows=[("t",)], fields=[("v", 16)])
        wire.query("SELECT nspname, pg_catalog.pg_is_other_temp_schema(n.oid) AS v "
                   f"FROM pg_catalog.pg_namespace n WHERE n.oid='{oid}'::oid ORDER BY nspname",
                   rows=[(ordinary, "t"), (duplicate, "t")], fields=[("nspname", 19), ("v", 16)], tag="SELECT 2")
    finally:
        fixture.write_bytes(original)
        fixture.replace(catalog)
    wire.query(f"SELECT pg_catalog.pg_is_other_temp_schema('{oid}'::oid) AS v",
               rows=[("f",)], fields=[("v", 16)])


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('sql_value_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference='--reference18' in sys.argv
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client)
        sock=server['sock'];user='alice';database='info'

    def connect():
        address = (host, port) if reference else ("127.0.0.1", server["port"])
        peer = socket.create_connection(address, timeout=runner.wire_timeout())
        try:
            if reference:
                client.startup_reference(peer, user, database, password=password)
            else:
                client.startup(peer, user, database)
        except BaseException:
            peer.close()
            raise
        return peer

    checked=0;failures=[];created=[]

    def check(sql,rows=None,oids=None,names=None,state=None,describe=False):
        nonlocal checked
        checked+=1
        if describe:
            parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
            sock.sendall(client.typed(b'P',parse)+client.typed(b'D',b'S\0')+client.typed(b'S'))
            messages=client.read_until_ready(sock)
        else:
            messages=client.simple_query(sock,sql)
        result=runner.decode_wire_result(messages,include_types=True)
        valid=result[1]==state
        if rows is not None:valid=valid and result[0]==rows
        if oids is not None:valid=valid and result[5]==oids
        if names is not None:valid=valid and result[3]==names
        if state:valid=valid and result[0]==[] and result[4] is None
        print('SQL_VALUE_KEYWORD',sql,'describe=',describe,result,'pass=',valid,flush=True)
        if not valid:failures.append((sql,result,rows,oids,names,state))
        return result

    try:
        keywords=['current_user','session_user','current_role','current_catalog','current_schema',
                  'current_date','current_timestamp','localtimestamp','current_time','localtime','user']
        oids=[19]*5+[1082,1184,1114,1266,1083,19]
        sql='SELECT '+','.join(keywords)
        result=check(sql,oids=oids,names=keywords)
        if result[1] is None:
            assert len(result[0])==1 and result[0][0][:5]==[user,user,user,database,'public'],result
            assert all(value is not None for value in result[0][0]),result
        check(sql,[],oids,keywords,describe=True)
        check('SELECT current_user=session_user,current_role=current_user,current_catalog=current_database(),current_schema=current_schema(),USER=current_user',
              [['t']*5],[16]*5)
        check('SELECT USER,user AS who,(USER)::NAME',[[user,user,user]],[19]*3,['user','who','user'])
        check('SELECT USER,user AS who,(USER)::NAME',[],[19]*3,['user','who','user'],describe=True)
        check('SELECT "user"',state='42703')
        check('SELECT "user"() WHERE false',state='42883')
        check('SELECT "current_user"(),pg_catalog."current_user"(),current_schema(),pg_catalog.current_schema()',
              [[user,user,'public','public']],[19]*4)
        check("SELECT CASE WHEN false THEN current_user ELSE 'kept'::NAME END",[['kept']],[19])
        for name,oid in [('current_timestamp',1184),('localtimestamp',1114),('current_time',1266),('localtime',1083)]:
            check('SELECT '+name+'(3)',oids=[oid],names=[name])
            check('SELECT '+name+'(3)',[],[oid],[name],describe=True)
        for invalid in ['current_user()','USER()','session_user()','current_role()','current_catalog()',
                        'current_date()','localtime()','localtimestamp()',
                        'current_timestamp(1+1)','current_timestamp(-1)']:
            check('SELECT '+invalid,state='42601')
        check('SELECT current_schema(1) WHERE false',state='42883')
        check('SELECT "current_date"() WHERE false',state='42883')
        result=check('CREATE FUNCTION sql_value_keyword_user() RETURNS NAME LANGUAGE sql AS $$ SELECT current_user $$')
        if result[1] is None:created.append('sql_value_keyword_user')
        check('SELECT sql_value_keyword_user()',[[user]],[19])
        check('SELECT sql_value_keyword_user()',[],[19],describe=True)
        result=check('CREATE FUNCTION sql_value_keyword_alias_user() RETURNS NAME LANGUAGE sql AS $$ SELECT USER $$')
        if result[1] is None:created.append('sql_value_keyword_alias_user')
        check('SELECT sql_value_keyword_alias_user()',[[user]],[19])
        check('SELECT sql_value_keyword_alias_user()',[],[19],describe=True)
        result=check('CREATE FUNCTION sql_value_keyword_schema() RETURNS NAME LANGUAGE sql AS $$ SELECT current_schema $$')
        if result[1] is None:created.append('sql_value_keyword_schema')
        check('SELECT sql_value_keyword_schema()',[["public"]],[19])
        result=check('CREATE FUNCTION sql_value_keyword_text_user() RETURNS TEXT LANGUAGE sql AS $$ SELECT current_user $$')
        if result[1] is None:created.append('sql_value_keyword_text_user')
        check('SELECT sql_value_keyword_text_user()',[[user]],[25])
        result=check('CREATE FUNCTION sql_value_keyword_text_schema() RETURNS TEXT LANGUAGE sql AS $$ SELECT current_schema $$')
        if result[1] is None:created.append('sql_value_keyword_text_schema')
        check('SELECT sql_value_keyword_text_schema()',[["public"]],[25])
        check_temporary_schema(client, sock)
        check_temp_inspection(client, sock, connect)
        if not reference:
            check_temp_catalog_change(client, sock)
            check_duplicate_namespace_oid(client, server)
        print('SQL_VALUE_KEYWORD_COMPLETE',checked,'FAILED',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:
                for name in created:client.simple_query(sock,'DROP FUNCTION '+name+'()')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
