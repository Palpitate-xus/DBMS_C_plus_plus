"""SQL SET search_path arguments are identifiers, not a preformatted GUC list."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid

# PostgreSQL 18.6 src/include/parser/kwlist.h: U=unreserved, C=column/type,
# R=reserved. These golden facts are checked against the actual reference,
# independently of the project's keyword classifier.
KEYWORDS = """
abort:U absent:U absolute:U access:U action:U add:U admin:U
after:U aggregate:U all:R also:U alter:U always:U analyse:R
analyze:R and:R any:R array:R as:R asc:R asensitive:U
assertion:U assignment:U asymmetric:R at:U atomic:U attach:U attribute:U
authorization:C backward:U before:U begin:U between:C bigint:C binary:C
bit:C boolean:C both:R breadth:U by:U cache:U call:U
called:U cascade:U cascaded:U case:R cast:R catalog:U chain:U
char:C character:C characteristics:U check:R checkpoint:U class:U close:U
cluster:U coalesce:C collate:R collation:C column:R columns:U comment:U
comments:U commit:U committed:U compression:U concurrently:C conditional:U configuration:U
conflict:U connection:U constraint:R constraints:U content:U continue:U conversion:U
copy:U cost:U create:R cross:C csv:U cube:U current:U
current_catalog:R current_date:R current_role:R current_schema:C current_time:R current_timestamp:R current_user:R
cursor:U cycle:U data:U database:U day:U deallocate:U dec:C
decimal:C declare:U default:R defaults:U deferrable:R deferred:U definer:U
delete:U delimiter:U delimiters:U depends:U depth:U desc:R detach:U
dictionary:U disable:U discard:U distinct:R do:R document:U domain:U
double:U drop:U each:U else:R empty:U enable:U encoding:U
encrypted:U end:R enforced:U enum:U error:U escape:U event:U
except:R exclude:U excluding:U exclusive:U execute:U exists:C explain:U
expression:U extension:U external:U extract:C false:R family:U fetch:R
filter:U finalize:U first:U float:C following:U for:R force:U
foreign:R format:U forward:U freeze:C from:R full:C function:U
functions:U generated:U global:U grant:R granted:U greatest:C group:R
grouping:C groups:U handler:U having:R header:U hold:U hour:U
identity:U if:U ilike:C immediate:U immutable:U implicit:U import:U
in:R include:U including:U increment:U indent:U index:U indexes:U
inherit:U inherits:U initially:R inline:U inner:C inout:C input:U
insensitive:U insert:U instead:U int:C integer:C intersect:R interval:C
into:R invoker:U is:C isnull:C isolation:U join:C json:C
json_array:C json_arrayagg:C json_exists:C json_object:C json_objectagg:C json_query:C json_scalar:C
json_serialize:C json_table:C json_value:C keep:U key:U keys:U label:U
language:U large:U last:U lateral:R leading:R leakproof:U least:C
left:C level:U like:C limit:R listen:U load:U local:U
localtime:R localtimestamp:R location:U lock:U locked:U logged:U mapping:U
match:U matched:U materialized:U maxvalue:U merge:U merge_action:C method:U
minute:U minvalue:U mode:U month:U move:U name:U names:U
national:C natural:C nchar:C nested:U new:U next:U nfc:U
nfd:U nfkc:U nfkd:U no:U none:C normalize:C normalized:U
not:R nothing:U notify:U notnull:C nowait:U null:R nullif:C
nulls:U numeric:C object:U objects:U of:U off:U offset:R
oids:U old:U omit:U on:R only:R operator:U option:U
options:U or:R order:R ordinality:U others:U out:C outer:C
over:U overlaps:C overlay:C overriding:U owned:U owner:U parallel:U
parameter:U parser:U partial:U partition:U passing:U password:U path:U
period:U placing:R plan:U plans:U policy:U position:C preceding:U
precision:C prepare:U prepared:U preserve:U primary:R prior:U privileges:U
procedural:U procedure:U procedures:U program:U publication:U quote:U quotes:U
range:U read:U real:C reassign:U recursive:U ref:U references:R
referencing:U refresh:U reindex:U relative:U release:U rename:U repeatable:U
replace:U replica:U reset:U restart:U restrict:U return:U returning:R
returns:U revoke:U right:C role:U rollback:U rollup:U routine:U
routines:U row:C rows:U rule:U savepoint:U scalar:U schema:U
schemas:U scroll:U search:U second:U security:U select:R sequence:U
sequences:U serializable:U server:U session:U session_user:R set:U setof:C
sets:U share:U show:U similar:C simple:U skip:U smallint:C
snapshot:U some:R source:U sql:U stable:U standalone:U start:U
statement:U statistics:U stdin:U stdout:U storage:U stored:U strict:U
string:U strip:U subscription:U substring:C support:U symmetric:R sysid:U
system:U system_user:R table:R tables:U tablesample:C tablespace:U target:U
temp:U template:U temporary:U text:U then:R ties:U time:C
timestamp:C to:R trailing:R transaction:U transform:U treat:C trigger:U
trim:C true:R truncate:U trusted:U type:U types:U uescape:U
unbounded:U uncommitted:U unconditional:U unencrypted:U union:R unique:R unknown:U
unlisten:U unlogged:U until:U update:U user:R using:R vacuum:U
valid:U validate:U validator:U value:U values:C varchar:C variadic:R
varying:U verbose:C version:U view:U views:U virtual:U volatile:U
when:R where:R whitespace:U window:R with:R within:U without:U
work:U wrapper:U write:U xml:U xmlattributes:C xmlconcat:C xmlelement:C
xmlexists:C xmlforest:C xmlnamespaces:C xmlparse:C xmlpi:C xmlroot:C xmlserialize:C
xmltable:C year:U yes:U zone:U
""".split()

CASES = [
    ("'public, pg_catalog'", '"public, pg_catalog"'),
    ("'Public'", '"Public"'), ("'public'", "public"),
    ("'\"two words\"'", '"""two words"""'),
    ("'a''b'", '"a\'b"'), ("'a,b'", '"a,b"'), ("''", '""'),
    ("'default'", '"default"'), ('"default"', '"default"'),
    ("'123'", '"123"'), ('"123"', '"123"'), ("123", "123"),
    ("- 123", "-123"), ("+123", "123"), ("0x4", "4"),
    ("0o17", "15"), ("0b101", "5"), ("1_234", "1234"),
    ("1.2", "1.2"), ("1e2", "1e2"), (".5", ".5"), ("-5.", "-5."),
    ("'$user'", '"$user"'), ("'$$'", '"$$"'),
    ("E'a\\tb'", '"a\tb"'), ("E'a\\x2cb'", '"a,b"'),
    ("$$a,b$$", '"a,b"'), ("'a'\n'b'", "ab"),
    ("'a' -- continue\n'b'", "ab"), ("E'a\\t'\n'b'", '"a\tb"'),
    ("'a', 'B', pg_catalog", 'a, "B", pg_catalog'),
    ('"a""b"', '"a""b"'), ('"public"', "public"),
    ("'中文'", '"中文"'), ("'line\nbreak'", '"line\nbreak"'),
    ("'tab\tname'", '"tab\tname"'), ("' a '", '" a "'),
    ("'a.b'", '"a.b"'), ("'a=b'", '"a=b"'),
]
NEGATIVES = ("public,", "public,,pg_catalog", "public junk", "'a' 'b'",
             "'a' /*\n*/ 'b'", "null", "select", "'a' + 'b'", "helper()",
             "DEFAULT,public", "public,DEFAULT", "B'01'", "1_.2")


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("search_path_values_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("SEARCH_PATH_VALUES", checks, repr(label), "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def request(sql, mode="simple"):
        if mode == "simple":
            return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHh", 0, 0, 1, mode))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def utility(sql, mode="simple", ready=b"I", state=None, tag=None):
        messages = request(sql, mode)
        result = runner.decode_wire_result(messages, include_types=True)
        kinds = [kind for kind, _ in messages]
        good = messages[-1] == (b"Z", ready) and result[1] == state and result[0] == []
        good = good and not any(kind in (b"T", b"D", b"N") for kind in kinds)
        if state:
            good = good and kinds.count(b"E") == 1 and b"C" not in kinds
        else:
            expected = tag or sql.split()[0].upper()
            good = good and [body for kind, body in messages if kind == b"C"] == [expected.encode() + b"\0"]
            good = good and b"E" not in kinds
            if mode != "simple":
                good = good and kinds.count(b"1") == kinds.count(b"2") == kinds.count(b"n") == 1
        report(good, (sql, mode), (result, kinds, messages[-1]))

    def show(value, mode="simple", ready=b"I"):
        messages = request("SHOW search_path", mode)
        kinds = [kind for kind, _ in messages]
        format_code = 0 if mode == "simple" else mode
        field = struct.pack("!H", 1) + b"search_path\0" + struct.pack("!IhIhih", 0, 0, 25, -1, -1, format_code)
        raw_value = value.encode()
        row = struct.pack("!Hi", 1, len(raw_value)) + raw_value
        good = [body for kind, body in messages if kind == b"T"] == [field]
        good = good and [body for kind, body in messages if kind == b"D"] == [row]
        good = good and messages[-1] == (b"Z", ready) and not any(kind in (b"E", b"N") for kind in kinds)
        good = good and [body for kind, body in messages if kind == b"C"] == [b"SHOW\0"]
        report(good, ("SHOW", value, mode), (runner.decode_wire_result(messages, include_types=True), kinds))

    def query_id(value, mode):
        messages = request("SELECT id FROM r145_items", mode)
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"T") and result[0] == [[str(value)]]
        good = good and result[1] is None and result[3] == ["id"] and result[5] == [23]
        good = good and result[4] == "SELECT 1"
        report(good, ("resolved schema id", value, mode), result)

    try:
        baseline = runner.decode_wire_result(client.simple_query(sock, "SHOW search_path"), include_types=True)
        assert baseline[1] is None and len(baseline[0]) == 1
        default_path = baseline[0][0][0]
        forms = ("SET search_path TO {}", "SET search_path = {}",
                 'SET SESSION "SEARCH_PATH" TO {}', 'SET SESSION "search_path" = {}')
        for mode in ("simple", 0):
            for form in forms:
                for literal, expected in CASES:
                    utility(form.format(literal), mode)
                    show(expected)
                    show(expected, 1)
        # Every keyword's quoted datum and bare spelling, including DEFAULT's
        # reset action and reserved-word failures with previous value retained.
        for index, item in enumerate(KEYWORDS):
            name, category = item.split(":")
            expected = name if category == "U" else '"' + name + '"'
            mode = "simple" if index % 2 == 0 else 0
            utility("SET search_path TO '" + name + "'", mode)
            show(expected)
            show(expected, 1)
            if name == "default":
                utility("SET search_path TO " + name, mode)
                show(default_path)
            elif category == "R" and name not in ("true", "false", "on"):
                utility("SET search_path TO " + name, mode, state="42601")
                show(expected)
            else:
                utility("SET search_path TO " + name, mode)
                show(expected)
        for mode in ("simple", 0):
            utility("SET search_path TO public", mode)
            for value in NEGATIVES:
                utility("SET search_path TO " + value, mode, state="42601")
                show("public", 1)
            utility("SET search_path FROM CURRENT", mode)
            show("public")
            utility("SET search_path TO DEFAULT", mode)
            show(default_path)
            utility("SET search_path TO 'a,b'", mode)
            utility("BEGIN", mode, ready=b"T")
            utility("SET LOCAL search_path TO 'Upper'", mode, ready=b"T")
            show('"Upper"', 1, b"T")
            utility("SAVEPOINT r145_path", mode, ready=b"T", tag="SAVEPOINT")
            utility("SET search_path TO 'a''b'", mode, ready=b"T")
            show('"a\'b"', 1, b"T")
            utility("ROLLBACK TO r145_path", mode, ready=b"T", tag="ROLLBACK")
            show('"Upper"', 1, b"T")
            utility("COMMIT", mode)
            show('"a,b"')
        # Names with commas/quotes/case select actual different stored rows.
        # Everything created on the reference is rolled back, including errors.
        prefix = "r145_" + uuid.uuid4().hex[:10]
        names = (prefix + " a,b", prefix + '"two words"', prefix + "Upper")
        identifier = lambda value: '"' + value.replace('"', '""') + '"'
        literal = lambda value: "'" + value.replace("'", "''") + "'"
        utility("BEGIN", ready=b"T")
        for index, name in enumerate(names):
            target = identifier(name)
            utility("CREATE SCHEMA " + target, ready=b"T", tag="CREATE SCHEMA")
            utility("CREATE TABLE " + target + ".r145_items(id INT)", ready=b"T", tag="CREATE TABLE")
            utility("INSERT INTO " + target + ".r145_items VALUES(" + str(index + 11) + ")", ready=b"T", tag="INSERT 0 1")
        for mode in ("simple", 0):
            for index, name in enumerate(names):
                utility("SET LOCAL search_path TO " + literal(name), mode, b"T")
                show(identifier(name), 1, b"T")
                query_id(index + 11, mode)
        utility("ROLLBACK")
        # FROM CURRENT is an assignment: bare/SESSION freezes a LOCAL
        # visible value as persistent, LOCAL does not, and ROLLBACK undoes it.
        for mode in ("simple", 0):
            for command in ("SET", "SET SESSION", "SET LOCAL"):
                utility("SET search_path TO public", mode)
                utility("BEGIN", mode, ready=b"T")
                utility("SET LOCAL search_path TO 'Upper'", mode, ready=b"T")
                utility(command + " search_path FROM CURRENT", mode, ready=b"T")
                show('"Upper"', 1, b"T")
                utility("COMMIT", mode)
                show("public" if command == "SET LOCAL" else '"Upper"', 1)
            utility("SET search_path TO public", mode)
            utility("BEGIN", mode, ready=b"T")
            utility("SET LOCAL search_path TO 'Upper'", mode, ready=b"T")
            utility("SET search_path FROM CURRENT", mode, ready=b"T")
            utility("ROLLBACK", mode)
            show("public", 1)
        expected_checks = 8 * len(CASES) * 3 + len(KEYWORDS) * 5 + 2 * (1 + 2 * len(NEGATIVES) + 4 + 11) + 29 + 54
        print("SEARCH_PATH_VALUES_COMPLETE", checks, "EXPECTED", expected_checks, "FAILURES", len(failures), flush=True)
        assert checks == expected_checks and not failures, (checks, expected_checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
