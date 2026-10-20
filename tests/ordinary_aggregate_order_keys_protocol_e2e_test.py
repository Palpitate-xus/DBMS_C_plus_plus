#!/usr/bin/env python3
"""Ordinary ordered aggregate regression:101 +31 +58 wire operations.

Values/effects/errors are checked against complete PostgreSQL18 runs. Preserve raw
protocol frames and exact operation count, including metadata-only requests.
"""

from compat.numeric_type_review import Review
import json
import re
import struct
import sys
import uuid
from quoted_unqualified_scalar_projection_protocol_e2e_test import matches


LABEL = "ORDINARY_AGGREGATE_ORDER_KEYS_PROTOCOL"
PLANNED = 190
MODES = ("simple", "extended", "binary")


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def cstring(body, offset):
    end = body.find(b"\0", offset)
    if end < 0:
        raise ValueError("unterminated protocol string")
    return body[offset:end].decode("utf-8"), end + 1


def diagnostic(body):
    fields, offset = {}, 0
    while offset < len(body):
        key = body[offset:offset + 1]
        offset += 1
        if key == b"\0":
            if offset != len(body):
                raise ValueError("diagnostic trailing bytes")
            break
        if key[0] not in b"SVCMDHPpqWstcdnFLR" or key.decode("ascii") in fields:
            raise ValueError("unknown/duplicate diagnostic field")
        value, offset = cstring(body, offset)
        fields[key.decode("ascii")] = value
    else:
        raise ValueError("diagnostic final terminator missing")
    if not fields.get("S") or not fields.get("M") or not re.fullmatch(r"[0-9A-Z]{5}", fields.get("C", "")):
        raise ValueError("diagnostic required fields missing")
    return fields


def description(body):
    if len(body) < 2:
        raise ValueError("RowDescription count missing")
    count, offset, fields = struct.unpack_from("!H", body)[0], 2, []
    for _ in range(count):
        name, offset = cstring(body, offset)
        if len(body) - offset < 18:
            raise ValueError("RowDescription field truncated")
        origin, attribute, oid, width, typmod, form = struct.unpack_from("!IhIhih", body, offset)
        offset += 18
        if form not in (0, 1):
            raise ValueError("RowDescription unknown format")
        fields.append(dict(name=name, tableOid=origin, attribute=attribute, oid=oid,
                           width=width, typmod=typmod, format=form))
    if offset != len(body):
        raise ValueError("RowDescription trailing bytes")
    return fields


def data_row(body):
    if len(body) < 2:
        raise ValueError("DataRow count missing")
    count, offset, values = struct.unpack_from("!H", body)[0], 2, []
    for _ in range(count):
        if len(body) - offset < 4:
            raise ValueError("DataRow length missing")
        length = struct.unpack_from("!i", body, offset)[0]
        offset += 4
        if length == -1:
            values.append(None)
        elif length < -1 or length > len(body) - offset:
            raise ValueError("DataRow invalid length/overrun")
        else:
            raw = body[offset:offset + length]
            offset += length
            try:
                value = raw.decode("utf-8")
            except UnicodeError:
                value = None
            values.append(dict(hex=raw.hex(), utf8=value))
    if offset != len(body):
        raise ValueError("DataRow trailing bytes")
    return values


def inspect(messages, operation):
    """Only protocol structure is required; SQL errors/results remain observations."""
    result = dict(frames=[dict(kind=kind.decode("ascii", "backslashreplace"), bodyHex=body.hex())
                          for kind, body in messages], descriptions=[], rows=[], errors=[], notices=[], tags=[])
    for kind, body in messages:
        if kind == b"T":
            result["descriptions"].append(description(body))
        elif kind == b"D":
            result["rows"].append(data_row(body))
        elif kind in (b"E", b"N"):
            fields = diagnostic(body)
            severity = fields.get("V", fields["S"])
            if severity != ("ERROR" if kind == b"E" else "NOTICE"):
                raise ValueError("unexpected diagnostic severity")
            result["errors" if kind == b"E" else "notices"].append(fields)
        elif kind == b"C":
            tag, end = cstring(body, 0)
            if end != len(body):
                raise ValueError("CommandComplete trailing bytes")
            result["tags"].append(tag)
    if messages[-1:] != [(b"Z", b"I")] or sum(kind == b"Z" for kind, _ in messages) != 1:
        raise ValueError("expected exactly one terminal ReadyI")
    if result["rows"] and (not result["descriptions"] or
            any(len(row) != len(result["descriptions"][-1]) for row in result["rows"])):
        raise ValueError("DataRow/RowDescription field count mismatch")
    visible = [(kind, body) for kind, body in messages if kind != b"N"]
    action, row_query = operation["action"], operation["query"]
    wanted_descriptor = b"T" if row_query else b"n"
    prefix = []
    if action in ("full", "parse"):
        prefix += [(b"1", "Parse"), (b"t", "DescribeStatement"), (wanted_descriptor, "DescribeStatement")]
    if action in ("full", "bind", "execute", "rebind"):
        prefix += [(b"2", "Bind"), (wanted_descriptor, "DescribePortal")]
    offset = 0
    for wanted, stage in prefix:
        if visible[offset][0] == b"E":
            if visible[offset + 1:] != [(b"Z", b"I")]:
                raise ValueError("error has extra frames")
            result["errorStage"] = stage
            return result
        kind, body = visible[offset]
        if kind != wanted or (kind in (b"1", b"2", b"n") and body != b"") or (kind == b"t" and body != b"\0\0"):
            raise ValueError("unexpected preparation frame/payload")
        offset += 1
    tail = visible[offset:-1]
    stage = "SimpleQuery(analysis-or-execution-unsplit)" if action == "simple" else "CloseStatement" if action == "close" else "Execute"
    result["errorStage"] = None
    if tail and tail[-1][0] == b"E":
        before_error = tail[:-1]
        if action == "simple" and row_query and before_error[:1] and before_error[0][0] == b"T":
            before_error = before_error[1:]
        if action in ("execute", "rebind") and before_error[-1:] and before_error[-1][0] == b"C":
            result["errorStage"] = "ClosePortal"
            before_error = before_error[:-1]
        elif action == "bind":
            result["errorStage"] = "ClosePortal"
        if action == "parse" or (action in ("bind", "close") and before_error) or any(kind != b"D" for kind, _ in before_error):
            raise ValueError("unexpected error position")
        result["errorStage"] = result["errorStage"] or stage
    elif action == "parse":
        if tail:
            raise ValueError("preparation performed extra work")
    elif action == "bind":
        if tail != [(b"3", b"")]:
            raise ValueError("metadata-only portal close sequence")
    elif action == "close":
        if tail != [(b"3", b"")]:
            raise ValueError("unexpected Close sequence")
    else:
        if action in ("execute", "rebind"):
            if tail[-1:] != [(b"3", b"")]:
                raise ValueError("executed portal not explicitly closed")
            tail = tail[:-1]
        if action == "simple" and row_query:
            if not tail or tail[0][0] != b"T":
                raise ValueError("simple query descriptor absent")
            tail = tail[1:]
        if not tail or tail[-1][0] != b"C" or any(kind != b"D" for kind, _ in tail[:-1]):
            raise ValueError("unexpected execution sequence")
        if not row_query and tail[:-1]:
            raise ValueError("utility unexpectedly produced rows")
        if row_query and (len(result["tags"]) != 1 or result["tags"][0] != "SELECT " + str(len(result["rows"]))):
            raise ValueError("SELECT count/tag inconsistent")
    return result


def compatible(row):
    operation, observed = row["operation"], row.get("observation")
    if not observed:
        return False
    oracle = operation["expected"]
    state = oracle.get("state")
    if state:
        stage = "SimpleQuery(analysis-or-execution-unsplit)" if operation["action"] == "simple" else "Parse"
        return ([error["C"] for error in observed["errors"]] == [state] and
                observed["errorStage"] == stage and not observed["rows"] and
                not observed["descriptions"] and not observed["tags"])
    if observed["errors"]:
        return False
    if not operation["query"]:
        return observed["tags"] == [operation["commandTag"]] and not observed["rows"] and not observed["descriptions"]
    if operation["action"] == "close":
        return not observed["rows"] and not observed["descriptions"] and not observed["tags"]
    oid, name, action = oracle["oid"], oracle.get("name", "v"), operation["action"]
    forms = [0, int(operation["binary"])] if action == "full" else [0] if action in ("simple", "parse") else [int(operation["binary"])]
    expected_fields = [[dict(name=name, tableOid=0, attribute=0, oid=oid,
                            width={20: 8, 701: 8, 1700: -1}[oid], typmod=-1, format=form)] for form in forms]
    if "physicalAttribute" in oracle:
        descriptions = observed["descriptions"]
        if len(descriptions) != len(forms) or any(len(fields) != 1 for fields in descriptions):
            return False
        origin = descriptions[0][0]["tableOid"]
        if origin <= 0:
            return False
        for fields in expected_fields:
            fields[0]["tableOid"] = origin
            fields[0]["attribute"] = oracle["physicalAttribute"]
    if observed["descriptions"] != expected_fields or observed["notices"]:
        return False
    if action in ("parse", "bind"):
        return not observed["rows"] and not observed["tags"]
    values = oracle["values"]
    binary = operation["binary"] and action != "simple"
    return (observed["tags"] == ["SELECT " + str(len(values))] and len(observed["rows"]) == len(values) and
            all(len(actual) == 1 and matches(None if actual[0] is None else bytes.fromhex(actual[0]["hex"]), expected, oid, binary)
                for actual, expected in zip(observed["rows"], values)))


class Collector:
    def __init__(self, review, schema):
        self.review, self.client, self.sock, self.schema = review, review.client, review.sock, schema
        self.dead, self.schema_owned, self.baseline_healthy = None, False, None
        self.attempted = self.sql_errors = self.not_sent = self.transport_failures = 0
        self.cleanup_completed = False
        self.setup_healthy, self.statement_health = True, {}
        self.seed_value = None

    def packet(self, operation):
        typed, action = self.client.typed, operation["action"]
        sql, name, portal = operation["sql"].encode("utf-8"), operation["statement"].encode("ascii"), operation["portal"].encode("ascii")
        if action == "simple":
            return typed(b"Q", sql + b"\0")
        packet = b""
        if action in ("full", "parse"):
            packet += typed(b"P", name + b"\0" + sql + b"\0\0\0") + typed(b"D", b"S" + name + b"\0")
        if action in ("full", "bind", "execute", "rebind"):
            packet += typed(b"B", portal + b"\0" + name + b"\0" + struct.pack("!HHHH", 0, 0, 1, int(operation["binary"])))
            packet += typed(b"D", b"P" + portal + b"\0")
        if action in ("full", "execute", "rebind"):
            packet += typed(b"E", portal + b"\0" + struct.pack("!I", 0))
        if action in ("bind", "execute", "rebind"):
            packet += typed(b"C", b"P" + portal + b"\0")
        if action == "close":
            packet += typed(b"C", b"S" + name + b"\0")
        return packet + typed(b"S")

    def run(self, operation):
        row = dict(index=self.review.controls + 1, operation=operation, compatibilityOracle=operation["expected"],
                   baselineHealthyBefore=self.baseline_healthy, schemaOwnershipConfirmed=self.schema_owned,
                   setupHealthyBefore=self.setup_healthy,
                   retainedParseDescribeHealthyBefore=self.statement_health.get(operation["statement"]))
        good, messages = False, []
        if self.dead is not None or (operation["cleanup"] and not self.schema_owned):
            self.not_sent += 1
            row.update(status="NOTSENT", reason=self.dead or "UUID namespace creation not confirmed; deletion not authorized")
        else:
            try:
                packet = self.packet(operation)
                row["frontendPacketHex"] = packet.hex()
                self.attempted += 1
                self.sock.sendall(packet)
                messages = self.client.read_until_ready(self.sock)
            except Exception as error:
                self.transport_failures += 1
                self.dead = type(error).__name__ + ": " + str(error)
                row.update(status="TRANSPORT_FAILURE", reason=self.dead)
            else:
                row["rawFrames"] = [dict(kind=kind.decode("ascii", "backslashreplace"), bodyHex=body.hex()) for kind, body in messages]
                try:
                    observation = inspect(messages, operation)
                    row.update(status="OBSERVED", observation=observation)
                    self.sql_errors += len(observation["errors"])
                    good = True
                    if operation["action"] == "parse":
                        self.statement_health[operation["statement"]] = not observation["errors"]
                        row["retainedParseDescribeHealthyAfter"] = self.statement_health[operation["statement"]]
                    if operation["setup"] or operation["cleanup"]:
                        good = not observation["errors"] and observation["tags"] == [operation["commandTag"]]
                    if operation["label"] == "create UUID schema" and good:
                        self.schema_owned = True
                    if operation["cleanup"] and good:
                        self.cleanup_completed = True
                    if operation["observer"]:
                        fields, values = observation["descriptions"], observation["rows"]
                        healthy = (not observation["errors"] and len(fields) == 1 and len(fields[0]) == 1 and
                                   fields[0][0]["oid"] == 20 and fields[0][0]["format"] == 0 and
                                   fields[0][0]["width"] == 8 and fields[0][0]["typmod"] == -1 and fields[0][0]["name"] == "v" and
                                   len(values) == 1 and len(values[0]) == 1 and values[0][0] is not None)
                        value = values[0][0]["utf8"] if healthy else None
                        healthy = healthy and bool(re.fullmatch(r"-?[0-9]+", value or ""))
                        sequence_value = int(value) if healthy else None
                        healthy = healthy and -(1 << 63) <= sequence_value < (1 << 63)
                        row["sequenceValue"] = sequence_value if healthy else None
                        row["observerOwnAdvanceIfHealthy"] = 1 if healthy else None
                        row["sequenceObserverHealthy"] = healthy
                        good = good and healthy
                        if operation["label"] == "sequence health seed":
                            self.seed_value = sequence_value if healthy else None
                        if operation["label"] == "sequence health confirmation":
                            delta = sequence_value - self.seed_value if healthy and self.seed_value is not None else None
                            row["observerPrimitiveObservedDelta"] = delta
                            row["observerPrimitiveRequiredDelta"] = 1
                            self.baseline_healthy = healthy and delta == 1
                            good = good and self.baseline_healthy
                        row["baselineHealthyAfter"] = self.baseline_healthy
                except Exception as error:
                    good = False
                    row.update(status="PROTOCOL_DECODE_FAILURE", reason=type(error).__name__ + ": " + str(error))
        row["collectionPrerequisitesAndStructureValid"] = good
        row["compatibilityPass"] = good and compatible(row)
        if (operation["setup"] or operation["label"] in ("sequence health seed", "sequence health confirmation")) and not good:
            self.setup_healthy = False
        print("R176_RAW " + json.dumps(row, sort_keys=True, separators=(",", ":")), flush=True)
        self.review.controls += 1
        if not good:
            self.review.failures.append(("COLLECTION_FAILURE", row["index"], operation["label"], row["status"], row.get("reason")))
        elif not row["compatibilityPass"]:
            self.review.failures.append(("SEMANTIC_FAILURE", row["index"], operation["label"], operation["expected"], row["observation"]))


def operation(label, sql, mode="simple", action=None, statement="", query=True,
              setup=False, tag=None, observer=False, cleanup=False, portal=""):
    return dict(label=label, sql=sql, mode=mode, action=action or ("simple" if mode == "simple" else "full"),
                statement=statement, binary=mode == "binary", query=query, setup=setup,
                commandTag=tag, observer=observer, cleanup=cleanup, portal=portal)


def plan(token, schema):
    table, empty, sequence = (schema + "." + name for name in ("rows", "empty_rows", "effects"))
    nextval = "pg_catalog.nextval(" + literal(sequence) + ")"
    observe = lambda label: operation(label, "SELECT " + nextval + " AS v", observer=True)
    steps = [operation("create UUID schema", "CREATE SCHEMA " + schema, query=False, setup=True, tag="CREATE SCHEMA"),
             operation("create ordinary rows", "CREATE TABLE " + table + "(id INTEGER PRIMARY KEY,n INTEGER,k INTEGER)", query=False, setup=True, tag="CREATE TABLE"),
             operation("create ordinary empty table", "CREATE TABLE " + empty + "(id INTEGER PRIMARY KEY,n INTEGER,k INTEGER)", query=False, setup=True, tag="CREATE TABLE"),
             operation("insert ordinary rows", "INSERT INTO " + table + " VALUES(1,2,3),(2,-1,1),(3,NULL,2)", query=False, setup=True, tag="INSERT 0 3"),
             operation("create actual sequence", "CREATE SEQUENCE " + sequence, query=False, setup=True, tag="CREATE SEQUENCE"),
             observe("sequence health seed"), observe("sequence health confirmation")]
    for function in ("sum", "count"):
        for source, tail, shape in ((table, "", "rows"), (empty, "", "actual empty"),
                                     (table, " WHERE false", "WHERE false"), (table, " LIMIT 0", "LIMIT0")):
            for mode in MODES:
                steps.append(operation("missing key " + function + " " + shape + " " + mode,
                    "SELECT " + function + "(n ORDER BY missing) AS v FROM " + source + tail, mode))
    for function in ("sum", "count"):
        for mode in ("extended", "binary"):
            name = "r176_" + token + "_" + function + "_" + mode
            sql = "SELECT " + function + "(n ORDER BY " + nextval + ") AS v FROM " + table
            steps += [operation("retained Parse/Describe " + function + " " + mode, sql, mode, "parse", name), observe("after Parse/Describe " + name),
                      operation("retained Bind/Describe " + function + " " + mode, sql, mode, "bind", name, portal=name + "_metadata"), observe("after Bind/Describe " + name),
                      operation("retained Bind/Execute " + function + " " + mode, sql, mode, "execute", name, portal=name + "_run1"), observe("after Execute " + name),
                      operation("retained repeated Bind/Execute " + function + " " + mode, sql, mode, "rebind", name, portal=name + "_run2"), observe("after repeated Bind/Execute " + name),
                      operation("close retained " + name, sql, mode, "close", name)]
    for function in ("sum", "count"):
        steps += [operation("simple reached " + function, "SELECT " + function + "(n ORDER BY " + nextval + ") AS v FROM " + table), observe("after simple reached " + function)]
        for source, suffix, filter_sql, shape in ((empty, "", "", "actual empty"), (table, " WHERE false", "", "WHERE false"),
             (table, " LIMIT 0", "", "LIMIT0"), (table, "", " FILTER(WHERE false)", "FILTER false"), (table, " WHERE id=3", "", "NULL input")):
            steps += [operation("demand " + function + " " + shape, "SELECT " + function + "(n ORDER BY " + nextval + ")" + filter_sql + " AS v FROM " + source + suffix), observe("after demand " + function + " " + shape)]
    floats = schema + ".floats"
    steps += [operation("create ordinary float8 table", "CREATE TABLE " + floats + "(id INTEGER PRIMARY KEY,k INTEGER,v DOUBLE PRECISION)", query=False, setup=True, tag="CREATE TABLE"),
              operation("insert exact float8 inputs", "INSERT INTO " + floats + " VALUES(1,2,1),(2,3,-9007199254740992),(3,1,9007199254740992)", query=False, setup=True, tag="INSERT 0 3"),
              operation("observe binary stored float8 inputs", "SELECT v AS v FROM " + floats + " ORDER BY k", "binary")]
    for direction in ("ASC", "DESC"):
        for mode in MODES:
            steps.append(operation("observe float8 SUM " + direction + " " + mode,
                "SELECT sum(v ORDER BY k " + direction + ") AS v FROM " + floats, mode))
    cleanup = operation("finally cleanup confirmed UUID schema", "DROP SCHEMA " + schema + " CASCADE", query=False, tag="DROP SCHEMA", cleanup=True)
    assert len(steps) + 1 == 101
    expected_sequence = 0
    effects = 0
    for item in steps:
        label = item["label"]
        expected = dict(oid=20, values=[])
        if item["observer"]:
            expected_sequence += 1 + effects
            expected["values"] = [expected_sequence]
            effects = 0
        elif label.startswith("missing key "):
            expected = dict(state="42703")
        elif label.startswith(("retained Bind/Execute ", "retained repeated Bind/Execute ", "simple reached ")):
            expected["values"] = [2 if " count" in label else 1]
            effects = 3
        elif label.startswith("demand "):
            expected["values"] = [] if "LIMIT0" in label else [0 if " count" in label else None]
            effects = 1 if "NULL input" in label else 0
        elif label == "observe binary stored float8 inputs":
            expected = dict(oid=701, values=[9007199254740992.0, 1.0, -9007199254740992.0], physicalAttribute=3)
        elif label.startswith("observe float8 SUM "):
            expected = dict(oid=701, values=[0.0 if " ASC " in label else 1.0])
        item["expected"] = expected
    cleanup["expected"] = dict()
    return steps, cleanup


def shared_plan(schema):
    table = schema + ".rows"
    sequence = "pg_catalog.nextval(" + literal(schema + ".effects") + ")"
    observe = lambda label: operation(label, "SELECT " + sequence + " AS v", observer=True)
    steps = [operation("create UUID schema", "CREATE SCHEMA " + schema, query=False, setup=True, tag="CREATE SCHEMA"),
             operation("create ordinary rows", "CREATE TABLE " + table + "(n INTEGER,k INTEGER)", query=False, setup=True, tag="CREATE TABLE"),
             operation("insert ordinary rows", "INSERT INTO " + table + " VALUES(2,3),(1,1),(NULL,2)", query=False, setup=True, tag="INSERT 0 3"),
             operation("create actual sequence", "CREATE SEQUENCE " + schema + ".effects", query=False, setup=True, tag="CREATE SEQUENCE"),
             observe("sequence health seed"), observe("sequence health confirmation")]
    cases = [("sum same argument and key", "sum("+sequence+" ORDER BY "+sequence+")", [12], 3, None),
             ("sum distinct shared producer", "sum(DISTINCT "+sequence+" ORDER BY "+sequence+")", [24], 3, None),
             ("count shared producer", "count("+sequence+" ORDER BY "+sequence+")", [3], 3, None),
             ("duplicate volatile keys", "sum(n ORDER BY "+sequence+","+sequence+")", [3], 3, None),
             ("independent argument producer", "sum("+sequence+" ORDER BY n)", [60], 3, None),
             ("distinct different key", "sum(DISTINCT n ORDER BY k)", [], 0, "42P10"),
             ("distinct actual same argument", "sum(DISTINCT n ORDER BY n)", [3], 0, None),
             ("numeric constants not output ordinals", "sum(n ORDER BY 1,2 DESC NULLS FIRST)", [3], 0, None),
             ("output alias not input column", "sum(n ORDER BY projected)", [], 0, "42703"),
             ("shared producer false filter", "sum("+sequence+" ORDER BY "+sequence+") FILTER(WHERE false)", [None], 0, None),
             ("shared producer LIMIT0", "sum("+sequence+" ORDER BY "+sequence+")", [], 0, None),
             ("explicit less operator", "sum(n ORDER BY k USING <)", [3], 0, None)]
    for item in steps:
        item["expected"] = dict(oid=20, values=[1 if item["label"] == "sequence health seed" else 2])
    value = 2
    for label, expression, values, effects, state in cases:
        item = operation(label, "SELECT " + expression + " AS projected FROM " + table +
                         (" LIMIT 0" if label == "shared producer LIMIT0" else ""))
        oid = 1700 if label in ("sum same argument and key", "sum distinct shared producer",
                                "independent argument producer", "shared producer false filter",
                                "shared producer LIMIT0") else 20
        item["expected"] = dict(state=state) if state else dict(oid=oid, name="projected", values=values)
        observer = observe("after " + label)
        value += effects + 1
        observer["expected"] = dict(oid=20, values=[value])
        steps.extend([item, observer])
    cleanup = operation("finally cleanup confirmed UUID schema", "DROP SCHEMA " + schema + " CASCADE",
                        query=False, tag="DROP SCHEMA", cleanup=True)
    cleanup["expected"] = dict()
    assert len(steps) + 1 == 31
    return steps, cleanup


def boundary_plan(schema):
    table = schema + ".rows"
    steps = [operation("create UUID schema", "CREATE SCHEMA " + schema, query=False, setup=True, tag="CREATE SCHEMA"),
             operation("create float boundary table", "CREATE TABLE " + table + "(id INTEGER,k INTEGER,v DOUBLE PRECISION)", query=False, setup=True, tag="CREATE TABLE"),
             operation("insert float boundary inputs", "INSERT INTO " + table + " VALUES(1,2,1),(2,3,-9007199254740992),(3,1,9007199254740992)", query=False, setup=True, tag="INSERT 0 3")]
    cases = [
        ("computed ascending aggregate", "sum(v ORDER BY k ASC)+0 AS v", 701, [0.0], None),
        ("computed descending aggregate", "sum(v ORDER BY k DESC)+0 AS v", 701, [1.0], None),
        ("opposite aggregates in arithmetic", "sum(v ORDER BY k ASC)+sum(v ORDER BY k DESC) AS v", 701, [1.0], None),
        ("different ordered aggregates are ambiguous aliases", "sum(v ORDER BY k ASC) AS a,sum(v ORDER BY k DESC) AS a", 701, [], "42702"),
        ("constant first multi-key ascending", "sum(v ORDER BY 1,k ASC) AS v", 701, [0.0], None),
        ("constant first multi-key descending", "sum(v ORDER BY 1,k DESC) AS v", 701, [1.0], None),
        ("actual null first key", "sum(v ORDER BY CASE WHEN id=3 THEN NULL ELSE k END NULLS FIRST) AS v", 701, [0.0], None),
        ("actual null last key", "sum(v ORDER BY CASE WHEN id=3 THEN NULL ELSE k END NULLS LAST) AS v", 701, [1.0], None),
        ("using greater sorts descending", "sum(v ORDER BY k USING >) AS v", 701, [1.0], None),
        ("count actual ordered values", "count(v ORDER BY k) AS v", 20, [3], None),
        ("distinct different key before LIMIT0", "sum(DISTINCT v ORDER BY k) AS v", 701, [], "42P10"),
        ("missing key before LIMIT0", "sum(v ORDER BY missing) AS v", 701, [], "42703"),
        ("builtin equality is not ordering", "sum(v ORDER BY k USING =) AS v", 701, [], "42809"),
        ("nested aggregate key before LIMIT0", "sum(v ORDER BY sum(k)) AS v", 701, [], "42803"),
        ("window key before LIMIT0", "sum(v ORDER BY row_number() OVER()) AS v", 701, [], "42803"),
        ("set-returning key before LIMIT0", "sum(v ORDER BY unnest(ARRAY[1,2])) AS v", 701, [], "0A000"),
        ("aggregate window does not support inside order", "sum(v ORDER BY k) OVER() AS v", 701, [], "0A000"),
        ("absent order expression", "sum(v ORDER BY) AS v", 701, [], "42601"),
    ]
    for item in steps:
        item["expected"] = dict()
    for label, expression, oid, values, state in cases:
        suffix = " ORDER BY a" if "ambiguous aliases" in label else " LIMIT 0" if "before LIMIT0" in label else ""
        for mode in MODES:
            item = operation(label+" "+mode,"SELECT "+expression+" FROM "+table+suffix,mode)
            item["expected"] = dict(state=state) if state else dict(oid=oid,values=values)
            steps.append(item)
    cleanup = operation("finally cleanup confirmed UUID schema", "DROP SCHEMA " + schema + " CASCADE",
                        query=False, tag="DROP SCHEMA", cleanup=True)
    cleanup["expected"] = dict()
    assert len(steps)+1 == 58
    return steps,cleanup


def main():
    try:
        review = Review(LABEL)
    except Exception as error:
        print("R176_DISCOVERY_TERMINAL " + json.dumps(dict(planned=PLANNED, recorded=0, attempted=0,
              startupFailure=type(error).__name__ + ": " + str(error), acceptance=False,
              harnessStartupOutsideInventory=True)), flush=True)
        print(LABEL + "_COMPLETE", 0, "PLANNED", PLANNED, "COLLECTION_FAILURES 1 NOT_AN_ACCEPTANCE_RESULT", flush=True)
        raise
    try:
        totals = dict(attempted=0,notSent=0,transportFailures=0,observedSqlErrors=0,
                      cleanupCompleted=True,transportBroken=False,setupHealthy=True,
                      sequenceBaselineHealthy=True,sequenceGroupsChecked=0)
        for group in ("original", "shared", "boundary"):
            token = uuid.uuid4().hex
            schema = identifier("r176_" + token)
            collector = Collector(review, schema)
            steps, cleanup = (shared_plan(schema) if group == "shared" else boundary_plan(schema)
                              if group == "boundary" else plan(token, schema))
            try:
                for item in steps:
                    collector.run(item)
            finally:
                collector.run(cleanup)
                totals["attempted"] += collector.attempted
                totals["notSent"] += collector.not_sent
                totals["transportFailures"] += collector.transport_failures
                totals["observedSqlErrors"] += collector.sql_errors
                totals["cleanupCompleted"] = totals["cleanupCompleted"] and collector.cleanup_completed
                totals["transportBroken"] = totals["transportBroken"] or collector.dead is not None
                totals["setupHealthy"] = totals["setupHealthy"] and collector.setup_healthy
                if group in ("original","shared"):
                    totals["sequenceBaselineHealthy"] = totals["sequenceBaselineHealthy"] and collector.baseline_healthy is True
                    totals["sequenceGroupsChecked"] += 1
    except Exception as error:
        review.failures.append(("COLLECTOR EXCEPTION", type(error).__name__, str(error)))
    finally:
        try:
            review.close()
        except Exception as error:
            review.failures.append(("CLOSE", type(error).__name__, str(error)))
    if review.controls != PLANNED:
        review.failures.append(("PLANNED INVENTORY", review.controls, PLANNED))
    print("R176_DISCOVERY_TERMINAL " + json.dumps(dict(planned=PLANNED, recorded=review.controls,
          failures=len(review.failures),
          candidateRegression=True,harnessStartupOutsideInventory=True,**totals)),flush=True)
    print(LABEL + "_COMPLETE", review.controls, "FAILURES", len(review.failures), flush=True)
    return 1 if review.failures else 0


if __name__ == "__main__":
    sys.exit(main())
