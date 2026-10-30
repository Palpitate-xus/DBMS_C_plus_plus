"""Strict TimeZone SET grammar and Parse/Execute error boundaries (PostgreSQL 18)."""
import importlib.util
import json
from pathlib import Path
import socket
import struct
import sys

# Factual PG18.6 pg_get_keywords categories; reserved value exceptions are
# TRUE/FALSE/ON. The reference server independently verifies every row.
KEYWORDS = json.loads("[[\"abort\",false],[\"absent\",false],[\"absolute\",false],[\"access\",false],[\"action\",false],[\"add\",false],[\"admin\",false],[\"after\",false],[\"aggregate\",false],[\"all\",true],[\"also\",false],[\"alter\",false],[\"always\",false],[\"analyse\",true],[\"analyze\",true],[\"and\",true],[\"any\",true],[\"array\",true],[\"as\",true],[\"asc\",true],[\"asensitive\",false],[\"assertion\",false],[\"assignment\",false],[\"asymmetric\",true],[\"at\",false],[\"atomic\",false],[\"attach\",false],[\"attribute\",false],[\"authorization\",false],[\"backward\",false],[\"before\",false],[\"begin\",false],[\"between\",false],[\"bigint\",false],[\"binary\",false],[\"bit\",false],[\"boolean\",false],[\"both\",true],[\"breadth\",false],[\"by\",false],[\"cache\",false],[\"call\",false],[\"called\",false],[\"cascade\",false],[\"cascaded\",false],[\"case\",true],[\"cast\",true],[\"catalog\",false],[\"chain\",false],[\"char\",false],[\"character\",false],[\"characteristics\",false],[\"check\",true],[\"checkpoint\",false],[\"class\",false],[\"close\",false],[\"cluster\",false],[\"coalesce\",false],[\"collate\",true],[\"collation\",false],[\"column\",true],[\"columns\",false],[\"comment\",false],[\"comments\",false],[\"commit\",false],[\"committed\",false],[\"compression\",false],[\"concurrently\",false],[\"conditional\",false],[\"configuration\",false],[\"conflict\",false],[\"connection\",false],[\"constraint\",true],[\"constraints\",false],[\"content\",false],[\"continue\",false],[\"conversion\",false],[\"copy\",false],[\"cost\",false],[\"create\",true],[\"cross\",false],[\"csv\",false],[\"cube\",false],[\"current\",false],[\"current_catalog\",true],[\"current_date\",true],[\"current_role\",true],[\"current_schema\",false],[\"current_time\",true],[\"current_timestamp\",true],[\"current_user\",true],[\"cursor\",false],[\"cycle\",false],[\"data\",false],[\"database\",false],[\"day\",false],[\"deallocate\",false],[\"dec\",false],[\"decimal\",false],[\"declare\",false],[\"default\",true],[\"defaults\",false],[\"deferrable\",true],[\"deferred\",false],[\"definer\",false],[\"delete\",false],[\"delimiter\",false],[\"delimiters\",false],[\"depends\",false],[\"depth\",false],[\"desc\",true],[\"detach\",false],[\"dictionary\",false],[\"disable\",false],[\"discard\",false],[\"distinct\",true],[\"do\",true],[\"document\",false],[\"domain\",false],[\"double\",false],[\"drop\",false],[\"each\",false],[\"else\",true],[\"empty\",false],[\"enable\",false],[\"encoding\",false],[\"encrypted\",false],[\"end\",true],[\"enforced\",false],[\"enum\",false],[\"error\",false],[\"escape\",false],[\"event\",false],[\"except\",true],[\"exclude\",false],[\"excluding\",false],[\"exclusive\",false],[\"execute\",false],[\"exists\",false],[\"explain\",false],[\"expression\",false],[\"extension\",false],[\"external\",false],[\"extract\",false],[\"false\",true],[\"family\",false],[\"fetch\",true],[\"filter\",false],[\"finalize\",false],[\"first\",false],[\"float\",false],[\"following\",false],[\"for\",true],[\"force\",false],[\"foreign\",true],[\"format\",false],[\"forward\",false],[\"freeze\",false],[\"from\",true],[\"full\",false],[\"function\",false],[\"functions\",false],[\"generated\",false],[\"global\",false],[\"grant\",true],[\"granted\",false],[\"greatest\",false],[\"group\",true],[\"grouping\",false],[\"groups\",false],[\"handler\",false],[\"having\",true],[\"header\",false],[\"hold\",false],[\"hour\",false],[\"identity\",false],[\"if\",false],[\"ilike\",false],[\"immediate\",false],[\"immutable\",false],[\"implicit\",false],[\"import\",false],[\"in\",true],[\"include\",false],[\"including\",false],[\"increment\",false],[\"indent\",false],[\"index\",false],[\"indexes\",false],[\"inherit\",false],[\"inherits\",false],[\"initially\",true],[\"inline\",false],[\"inner\",false],[\"inout\",false],[\"input\",false],[\"insensitive\",false],[\"insert\",false],[\"instead\",false],[\"int\",false],[\"integer\",false],[\"intersect\",true],[\"interval\",false],[\"into\",true],[\"invoker\",false],[\"is\",false],[\"isnull\",false],[\"isolation\",false],[\"join\",false],[\"json\",false],[\"json_array\",false],[\"json_arrayagg\",false],[\"json_exists\",false],[\"json_object\",false],[\"json_objectagg\",false],[\"json_query\",false],[\"json_scalar\",false],[\"json_serialize\",false],[\"json_table\",false],[\"json_value\",false],[\"keep\",false],[\"key\",false],[\"keys\",false],[\"label\",false],[\"language\",false],[\"large\",false],[\"last\",false],[\"lateral\",true],[\"leading\",true],[\"leakproof\",false],[\"least\",false],[\"left\",false],[\"level\",false],[\"like\",false],[\"limit\",true],[\"listen\",false],[\"load\",false],[\"local\",false],[\"localtime\",true],[\"localtimestamp\",true],[\"location\",false],[\"lock\",false],[\"locked\",false],[\"logged\",false],[\"mapping\",false],[\"match\",false],[\"matched\",false],[\"materialized\",false],[\"maxvalue\",false],[\"merge\",false],[\"merge_action\",false],[\"method\",false],[\"minute\",false],[\"minvalue\",false],[\"mode\",false],[\"month\",false],[\"move\",false],[\"name\",false],[\"names\",false],[\"national\",false],[\"natural\",false],[\"nchar\",false],[\"nested\",false],[\"new\",false],[\"next\",false],[\"nfc\",false],[\"nfd\",false],[\"nfkc\",false],[\"nfkd\",false],[\"no\",false],[\"none\",false],[\"normalize\",false],[\"normalized\",false],[\"not\",true],[\"nothing\",false],[\"notify\",false],[\"notnull\",false],[\"nowait\",false],[\"null\",true],[\"nullif\",false],[\"nulls\",false],[\"numeric\",false],[\"object\",false],[\"objects\",false],[\"of\",false],[\"off\",false],[\"offset\",true],[\"oids\",false],[\"old\",false],[\"omit\",false],[\"on\",true],[\"only\",true],[\"operator\",false],[\"option\",false],[\"options\",false],[\"or\",true],[\"order\",true],[\"ordinality\",false],[\"others\",false],[\"out\",false],[\"outer\",false],[\"over\",false],[\"overlaps\",false],[\"overlay\",false],[\"overriding\",false],[\"owned\",false],[\"owner\",false],[\"parallel\",false],[\"parameter\",false],[\"parser\",false],[\"partial\",false],[\"partition\",false],[\"passing\",false],[\"password\",false],[\"path\",false],[\"period\",false],[\"placing\",true],[\"plan\",false],[\"plans\",false],[\"policy\",false],[\"position\",false],[\"preceding\",false],[\"precision\",false],[\"prepare\",false],[\"prepared\",false],[\"preserve\",false],[\"primary\",true],[\"prior\",false],[\"privileges\",false],[\"procedural\",false],[\"procedure\",false],[\"procedures\",false],[\"program\",false],[\"publication\",false],[\"quote\",false],[\"quotes\",false],[\"range\",false],[\"read\",false],[\"real\",false],[\"reassign\",false],[\"recursive\",false],[\"ref\",false],[\"references\",true],[\"referencing\",false],[\"refresh\",false],[\"reindex\",false],[\"relative\",false],[\"release\",false],[\"rename\",false],[\"repeatable\",false],[\"replace\",false],[\"replica\",false],[\"reset\",false],[\"restart\",false],[\"restrict\",false],[\"return\",false],[\"returning\",true],[\"returns\",false],[\"revoke\",false],[\"right\",false],[\"role\",false],[\"rollback\",false],[\"rollup\",false],[\"routine\",false],[\"routines\",false],[\"row\",false],[\"rows\",false],[\"rule\",false],[\"savepoint\",false],[\"scalar\",false],[\"schema\",false],[\"schemas\",false],[\"scroll\",false],[\"search\",false],[\"second\",false],[\"security\",false],[\"select\",true],[\"sequence\",false],[\"sequences\",false],[\"serializable\",false],[\"server\",false],[\"session\",false],[\"session_user\",true],[\"set\",false],[\"setof\",false],[\"sets\",false],[\"share\",false],[\"show\",false],[\"similar\",false],[\"simple\",false],[\"skip\",false],[\"smallint\",false],[\"snapshot\",false],[\"some\",true],[\"source\",false],[\"sql\",false],[\"stable\",false],[\"standalone\",false],[\"start\",false],[\"statement\",false],[\"statistics\",false],[\"stdin\",false],[\"stdout\",false],[\"storage\",false],[\"stored\",false],[\"strict\",false],[\"string\",false],[\"strip\",false],[\"subscription\",false],[\"substring\",false],[\"support\",false],[\"symmetric\",true],[\"sysid\",false],[\"system\",false],[\"system_user\",true],[\"table\",true],[\"tables\",false],[\"tablesample\",false],[\"tablespace\",false],[\"target\",false],[\"temp\",false],[\"template\",false],[\"temporary\",false],[\"text\",false],[\"then\",true],[\"ties\",false],[\"time\",false],[\"timestamp\",false],[\"to\",true],[\"trailing\",true],[\"transaction\",false],[\"transform\",false],[\"treat\",false],[\"trigger\",false],[\"trim\",false],[\"true\",true],[\"truncate\",false],[\"trusted\",false],[\"type\",false],[\"types\",false],[\"uescape\",false],[\"unbounded\",false],[\"uncommitted\",false],[\"unconditional\",false],[\"unencrypted\",false],[\"union\",true],[\"unique\",true],[\"unknown\",false],[\"unlisten\",false],[\"unlogged\",false],[\"until\",false],[\"update\",false],[\"user\",true],[\"using\",true],[\"vacuum\",false],[\"valid\",false],[\"validate\",false],[\"validator\",false],[\"value\",false],[\"values\",false],[\"varchar\",false],[\"variadic\",true],[\"varying\",false],[\"verbose\",false],[\"version\",false],[\"view\",false],[\"views\",false],[\"virtual\",false],[\"volatile\",false],[\"when\",true],[\"where\",true],[\"whitespace\",false],[\"window\",true],[\"with\",true],[\"within\",false],[\"without\",false],[\"work\",false],[\"wrapper\",false],[\"write\",false],[\"xml\",false],[\"xmlattributes\",false],[\"xmlconcat\",false],[\"xmlelement\",false],[\"xmlexists\",false],[\"xmlforest\",false],[\"xmlnamespaces\",false],[\"xmlparse\",false],[\"xmlpi\",false],[\"xmlroot\",false],[\"xmlserialize\",false],[\"xmltable\",false],[\"year\",false],[\"yes\",false],[\"zone\",false]]")
POSITIVES = json.loads("[[\"SET TimeZone='UTC'\",\"UTC\"],[\"SET SESSION TimeZone='Asia/Shanghai'\",\"Asia/Shanghai\"],[\"SET \\\"TimeZone\\\"='UTC'\",\"UTC\"],[\"SET SESSION \\\"tImEzOnE\\\" TO 'Asia/Kathmandu'\",\"Asia/Kathmandu\"],[\"SET TimeZone TO'UTC'\",\"UTC\"],[\"SET TIME ZONE'UTC'\",\"UTC\"],[\"SET SESSION TIME ZONE'UTC'\",\"UTC\"],[\"set/**/timezone/**/=/**/'UTC'\",\"UTC\"],[\"SET TimeZone = - 5.5\",\"<-05:30>+05:30\"],[\"SET SESSION TimeZone=+ 5.5\",\"<+05:30>-05:30\"],[\"SET TimeZone=5.e0\",\"<+05>-05\"],[\"SET TimeZone TO E'UT\\\\x43'\",\"UTC\"],[\"SET TimeZone TO $q$UTC$q$\",\"UTC\"],[\"SET TimeZone TO 'UT'\\n'C'\",\"UTC\"],[\"SET TimeZone TO 'UT' -- continuation\\n'C'\",\"UTC\"],[\"SET TimeZone FROM CURRENT\",\"Asia/Shanghai\"],[\"SET SESSION TimeZone FROM CURRENT\",\"Asia/Shanghai\"],[\"SET \\\"TimeZone\\\" FROM CURRENT\",\"Asia/Shanghai\"],[\"SET TimeZone=DEFAULT\",null],[\"SET SESSION TimeZone=DEFAULT\",null],[\"SET \\\"TimeZone\\\"=DEFAULT\",null],[\"SET TIME ZONE DEFAULT\",null],[\"SET SESSION TIME ZONE LOCAL\",null],[\"SET SESSION\\\"TimeZone\\\"='UTC'\",\"UTC\"],[\"SET \\\"TiMeZoNe\\\"TO'UTC'\",\"UTC\"],[\"SET\\\"TimeZone\\\"='UTC'\",\"UTC\"]]")
SYNTAX_ERRORS = json.loads("[\"SET TimeZone 'UTC'\",\"SET SESSION TimeZone 'UTC'\",\"SET \\\"TimeZone\\\" 'UTC'\",\"SET TimeZone DEFAULT\",\"SET SESSION TimeZone DEFAULT\",\"SET TIME ZONE = 'UTC'\",\"SET SESSION TIME ZONE = 'UTC'\",\"SET TIME ZONE = DEFAULT\",\"SET SESSION TIME ZONE = DEFAULT\",\"SET TIME ZONE TO 'UTC'\",\"SET SESSION TIME ZONE TO DEFAULT\",\"SET TimeZone\",\"SET TimeZone TO\",\"SET TimeZone=\",\"SET TIME ZONE\",\"SET TimeZone TO TO 'UTC'\",\"SET TimeZone == 'UTC'\",\"SET TimeZone TO 'UTC' junk\",\"SET TIME ZONE 'UTC' junk\",\"SET TimeZone TO 'UTC' 'GMT'\",\"SET TimeZone TO 'UT' /*\\n*/ 'C'\",\"SET TimeZone TO 'UTC',\",\"SET TimeZone TO ,'UTC'\",\"SET TimeZone TO DEFAULT,'UTC'\",\"SET TIME ZONE 'UTC','GMT'\",\"SET TimeZone TO Asia/Shanghai\",\"SET TimeZone TO +05:30\",\"SET TimeZone TO (5)\",\"SET TimeZone TO 5+1\",\"SET TimeZone TO $1\",\"SET TimeZone TO helper()\",\"SET TimeZone FROM\",\"SET TimeZone FROM DEFAULT\",\"SET TIME ZONE FROM CURRENT\",\"SET TimeZone FROM CURRENT junk\",\"SET TimeZone TO INTERVAL '+05:30'\",\"SET TIME ZONE INTERVAL '+05:30' DAY\",\"SET TIME ZONE INTERVAL '+05:30' SECOND\",\"SET TimeZone DEFAULT; SHOW TimeZone\",\"SET TIME ZONE = DEFAULT; SHOW TimeZone\",\"SETTINGS TimeZone='UTC'\",\"SETUP TimeZone DEFAULT\"]")
VALUE_ERRORS = json.loads("[\"SET TimeZone TO 'UTC','GMT'\",\"SET SESSION TimeZone = 'UTC','GMT'\",\"SET TimeZone TO 'DEFAULT'\",\"SET TIME ZONE 'LOCAL'\",\"SET TimeZone TO \\\"DEFAULT\\\"\",\"SET TIME ZONE \\\"LOCAL\\\"\",\"SET TimeZone TO LOCAL\",\"SET SESSION TimeZone=LOCAL\",\"SET TimeZone TO 'TO UTC'\",\"SET TIME ZONE 'Invalid/No_Such_Zone'\",\"SET TimeZone TO 'UTC','Invalid/No_Such_Zone'\"]")


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("timezone_grammar_runner", root / "tests/compat/pg_diff_runner.py")
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
        print("TIMEZONE_GRAMMAR", checks, label, "match", good,
              repr(detail) if not good else "", flush=True)
        if not good:
            failures.append((label, detail))

    def request(sql, extended):
        if not extended:
            return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def utility(sql, extended, state=None):
        messages = request(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        kinds = [kind for kind, _ in messages]
        good = messages[-1] == (b"Z", b"I") and result[1] == state and result[0] == []
        good = good and not any(kind in (b"T", b"D", b"N") for kind in kinds)
        if extended:
            count = 0 if state == "42601" else 1
            good = good and kinds.count(b"1") == kinds.count(b"2") == kinds.count(b"n") == count
        if state:
            good = good and kinds.count(b"E") == 1 and b"C" not in kinds
        else:
            good = good and b"E" not in kinds and [body for kind, body in messages if kind == b"C"] == [b"SET\0"]
        report(good, ("Extended " if extended else "Simple ") + sql, (result, kinds))

    def show(expected=None):
        messages = client.simple_query(sock, "SHOW TimeZone")
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[1] is None
        good = good and result[3] == ["TimeZone"] and result[5] == [25] and result[4] == "SHOW"
        good = good and len(result[0]) == 1 and len(result[0][0]) == 1
        if expected is not None:
            good = good and result[0] == [[expected]]
        report(good, "SHOW expected=" + repr(expected), result)
        return result[0][0][0]

    def check(sql, state, zone):
        for extended in (False, True):
            utility("SET TimeZone TO 'Asia/Shanghai'", extended)
            utility(sql, extended, state)
            show(zone)

    try:
        assert len(KEYWORDS) == 494 and len(set(word for word, _ in KEYWORDS)) == 494
        assert len(POSITIVES) == 26 and len(SYNTAX_ERRORS) == 42 and len(VALUE_ERRORS) == 11
        baseline = show()
        for sql, zone in POSITIVES:
            check(sql, None, baseline if zone is None else zone)
        for sql in SYNTAX_ERRORS:
            check(sql, "42601", "Asia/Shanghai")
        for sql in VALUE_ERRORS:
            check(sql, "22023", "Asia/Shanghai")
        for word, reserved in KEYWORDS:
            generic_state = None if word == "default" else ("42601" if reserved and word not in ("true", "false", "on") else "22023")
            check("SET TimeZone TO " + word, generic_state, baseline if generic_state is None else "Asia/Shanghai")
            dedicated_state = None if word in ("default", "local") else "42601"
            check("SET TIME ZONE " + word, dedicated_state, baseline if dedicated_state is None else "Asia/Shanghai")
        print("TIMEZONE_GRAMMAR_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 6403 and not failures, (checks, len(failures), failures[:10])
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
