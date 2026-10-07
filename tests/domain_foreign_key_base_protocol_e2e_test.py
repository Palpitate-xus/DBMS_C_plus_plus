"""Domains retain constraints/identity, while FK comparisons use their base."""
import importlib.util
from pathlib import Path
import socket
import sys

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('domain_fk_runner',repo/'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
c = r.load_protocol_client()
reference = '--reference18' in sys.argv
server = None
if reference:
    host, port, user, database, password = r._reference_connection_settings()
    sock = socket.create_connection((host,port),timeout=15)
    c.startup_reference(sock,user,database,password=password)
else:
    server = r.start_ours(c); sock = server['sock']
failures = []

def q(sql, rows=None, state=None, oids=None):
    result = r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    print('DOMAIN_FK',sql,result,flush=True)
    if result[1] != state or (rows is not None and result[0] != rows) or (oids is not None and result[5] != oids):
        raise AssertionError((sql,result,rows,state,oids))
    return result

def bad(sql,state):
    q('SAVEPOINT domain_fk_error')
    try: q(sql,state=state)
    finally:
        q('ROLLBACK TO SAVEPOINT domain_fk_error')
        q('RELEASE SAVEPOINT domain_fk_error')

def run(name,body):
    q('SAVEPOINT domain_fk_case')
    try: body()
    except AssertionError as error:
        failures.append((name,str(error)))
        print('DOMAIN_FK_FAILURE',failures[-1],flush=True)
    finally:
        q('ROLLBACK TO SAVEPOINT domain_fk_case')
        q('RELEASE SAVEPOINT domain_fk_case')

def domain_parent():
    # The original successful PG18/rejected candidate declaration is unchanged.
    q('CREATE TEMP TABLE d_follow_child(v INT REFERENCES d_follow_parent(v))')
    q('INSERT INTO d_follow_child VALUES(3)')
    q('SELECT v FROM d_follow_child',[['3']],oids=[23])
    bad('INSERT INTO d_follow_child VALUES(4)','23503')
    bad('DELETE FROM d_follow_parent WHERE v=3','23503')

def base_parent():
    q('CREATE TEMP TABLE domain_fk_child(v d_follow REFERENCES domain_fk_base(v))')
    q('INSERT INTO domain_fk_child VALUES(3)')
    q('SELECT v FROM domain_fk_child',[['3']],oids=[23])
    bad('INSERT INTO domain_fk_child VALUES(NULL)','23502')
    bad('INSERT INTO domain_fk_child VALUES(-1)','23514')
    bad('INSERT INTO domain_fk_child VALUES(4)','23503')

def different_domains():
    q('CREATE TEMP TABLE domain_fk_other(v domain_fk_small REFERENCES d_follow_parent(v))')
    q('INSERT INTO domain_fk_other VALUES(3)')
    bad('INSERT INTO domain_fk_other VALUES(11)','23514')
    q('SELECT v FROM domain_fk_other',[['3']],oids=[23])

def nested_domain():
    q('CREATE TEMP TABLE domain_fk_nested(v domain_fk_chain REFERENCES domain_fk_base(v))')
    q('INSERT INTO domain_fk_nested VALUES(3)')
    bad('INSERT INTO domain_fk_nested VALUES(NULL)','23502')
    bad('INSERT INTO domain_fk_nested VALUES(11)','23514')
    q('SELECT v FROM domain_fk_nested',[['3']],oids=[23])

def composite():
    q('CREATE TEMP TABLE domain_fk_combo(v d_follow,t TEXT,PRIMARY KEY(v,t))')
    q("INSERT INTO domain_fk_combo VALUES(3,'x')")
    q('CREATE TEMP TABLE domain_fk_combo_child(v INT,t TEXT,FOREIGN KEY(v,t) REFERENCES domain_fk_combo(v,t))')
    q("INSERT INTO domain_fk_combo_child VALUES(3,'x')")
    bad("INSERT INTO domain_fk_combo_child VALUES(3,'bad')",'23503')
    q('SELECT v,t FROM domain_fk_combo_child',[['3','x']],oids=[23,25])

def plain_guard():
    q('CREATE TEMP TABLE domain_fk_plain(v INT REFERENCES domain_fk_base(v))')
    q('INSERT INTO domain_fk_plain VALUES(3)')
    bad('INSERT INTO domain_fk_plain VALUES(4)','23503')
    q('SELECT v FROM domain_fk_plain',[['3']],oids=[23])

try:
    if reference: q('SHOW server_version_num',[['180006']])
    q('BEGIN')
    q('CREATE DOMAIN d_follow AS INT DEFAULT 1 NOT NULL CHECK(VALUE > 0)')
    q('CREATE DOMAIN domain_fk_small AS INT CHECK(VALUE < 10)')
    q('CREATE DOMAIN domain_fk_chain AS d_follow CHECK(VALUE < 10)')
    q('CREATE TEMP TABLE d_follow_parent(v d_follow PRIMARY KEY)')
    q('CREATE TEMP TABLE domain_fk_base(v INT PRIMARY KEY)')
    q('INSERT INTO d_follow_parent VALUES(3)')
    q('INSERT INTO domain_fk_base VALUES(3)')
    for name,body in [('domain_parent_base_child',domain_parent),('base_parent_domain_child',base_parent),
                      ('different_domains',different_domains),('nested_domain',nested_domain),
                      ('composite',composite),('plain_guard',plain_guard)]:
        run(name,body)
    q('SELECT v FROM d_follow_parent',[['3']],oids=[23])
    q('SELECT v FROM domain_fk_base',[['3']],oids=[23])
    q('ROLLBACK')
    print('DOMAIN_FK_FAILURES',failures,flush=True)
    assert not failures,failures
finally:
    if server: r.stop_ours(server)
    else:
        c.simple_query(sock,'ROLLBACK')
        sock.close()
