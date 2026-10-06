#include "utils/plpgsql.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <utility>

namespace {
using Result = dbms::PlPgsqlQueryResult;
Result rows(size_t count, std::vector<std::optional<std::string>> values) {
    Result result;
    result.ok = true;
    result.columnCount = values.size();
    result.rowCount = count;
    result.firstRow = count ? std::move(values) : std::vector<std::optional<std::string>>{};
    return result;
}
}

int main() {
    dbms::PlPgsqlHost host;
    std::string seen;
    std::vector<std::string> targets;
    int status = 0;
    host.selectInto = [&](const std::string& query, const std::vector<std::string>& names,
                          std::map<std::string, std::string>& vars) {
        seen = query;
        targets = names;
        if (status == 0) vars[names.at(0)] = "99";
        return status;
    };
    std::string value, error;
    bool isNull = false;
    assert(dbms::PlPgsql::run("DECLARE n INT; BEGIN SELECT id INTO n FROM ranges; RETURN n; END;",
                            {}, host, value, error, nullptr, &isNull));
    assert(seen == "id FROM ranges");
    assert(targets == std::vector<std::string>{"n"});
    assert(value == "99" && !isNull);
    assert(dbms::PlPgsql::run("DECLARE n INT := NULL; BEGIN SELECT 99 INTO n; RETURN n; END;",
                            {}, host, value, error, nullptr, &isNull));
    assert(value == "99" && !isNull);
    status = 1;
    assert(dbms::PlPgsql::run("DECLARE n INT := 7; BEGIN SELECT 99 INTO n; RETURN n; END;",
                            {}, host, value, error, nullptr, &isNull));
    assert(isNull);

    // Preferred callback receives a complete query, never a SELECT prefix
    // or an INTO target contaminated by FROM/WHERE/ORDER BY.
    Result result = rows(1, {"99"});
    int calls = 0;
    host.query = [&](const std::string& sql) {
        seen = sql;
        ++calls;
        return result;
    };
    std::string state, returnType;
    auto run = [&](const std::string& body) {
        return dbms::PlPgsql::run(body, {}, host, value, error, nullptr,
                                 &isNull, nullptr, &state, &returnType);
    };
    assert(run("DECLARE n INT; BEGIN SELECT id INTO n FROM ranges WHERE id=2 ORDER BY id; RETURN n; END;"));
    assert(seen == "SELECT id FROM ranges WHERE id=2 ORDER BY id");
    assert(value == "99" && !isNull && state.empty());
    assert(run("DECLARE n INT; BEGIN SELECT INTO n 99; RETURN n; END;"));
    assert(seen == "SELECT 99");
    assert(run("DECLARE n INT; BEGIN SELECT id FROM ranges WHERE id=2 INTO n; RETURN n; END;"));
    assert(seen == "SELECT id FROM ranges WHERE id=2");
    assert(run("DECLARE n INT; BEGIN WITH c AS (SELECT 99 AS id) SELECT id INTO n FROM c; RETURN n; END;"));
    assert(seen == "WITH c AS (SELECT 99 AS id) SELECT id FROM c");
    assert(run("DECLARE n INT; BEGIN SELECT id\nINTO\nSTRICT\nn\nFROM ranges; RETURN n; END;"));
    assert(seen == "SELECT id\nFROM ranges");

    // NULL state is independent of textual spelling, and switches in both
    // directions when a previously-NULL variable receives a row.
    result = rows(1, {std::nullopt});
    assert(run("DECLARE n TEXT := 'old'; BEGIN SELECT NULL INTO n; RETURN n; END;"));
    assert(isNull);
    for (const std::string datum : {"null", "", "O'Reilly", "00123", "1.000"}) {
        result = rows(1, {datum});
        assert(run("DECLARE n TEXT := NULL; BEGIN SELECT 'x' INTO n; RETURN n; END;"));
        assert(!isNull && value == datum);
    }
    result = rows(0, {"unused"});
    assert(run("DECLARE n TEXT := 'old'; BEGIN SELECT 'x' INTO n WHERE FALSE; RETURN n; END;"));
    assert(isNull);
    assert(run("DECLARE n INT; BEGIN RETURN n; END;"));
    assert(isNull);  // no-default declarations exist and start as SQL NULL
    assert(run("DECLARE z INT := 4; a INT := z + 1; BEGIN RETURN a; END;"));
    assert(value == "5" && !isNull);  // declaration order, not map order

    // PostgreSQL's default variable-list assignment truncates extra columns
    // and fills missing columns with NULL, including INTO STRICT.
    result = rows(1, {"1", "2"});
    assert(run("DECLARE n INT; BEGIN SELECT 1,2 INTO n; RETURN n; END;"));
    assert(value == "1" && !isNull);
    result = rows(1, {"1"});
    assert(run("DECLARE n INT; v TEXT := 'old'; BEGIN SELECT 1 INTO STRICT n,v; RETURN v; END;"));
    assert(isNull);
    result = rows(1, {"1", "second"});
    assert(run("DECLARE n INT; v TEXT; BEGIN SELECT 1,'second' INTO n,v; RETURN v; END;"));
    assert(value == "second" && !isNull);
    result = rows(0, {"1"});
    assert(run("DECLARE n INT := 7; v TEXT := 'old'; BEGIN SELECT 1 INTO n,v; RETURN v; END;"));
    assert(isNull);

    // Strict cardinality errors and host SQLSTATE survive the boundary.
    assert(!run("DECLARE n INT; BEGIN SELECT 1 INTO STRICT n; RETURN n; END;"));
    assert(state == "P0002");
    result = rows(2, {"1"});
    assert(!run("DECLARE n INT; BEGIN SELECT 1 INTO STRICT n; RETURN n; END;"));
    assert(state == "P0003");
    assert(run("DECLARE n INT; BEGIN SELECT 1 INTO n; RETURN n; END;"));
    assert(value == "1" && !isNull);
    assert(run("DECLARE n INT; BEGIN SELECT 1 INTO n; RETURN FOUND; END;"));
    assert(value == "t" && !isNull);
    result = rows(0, {"1"});
    assert(run("DECLARE n INT; BEGIN SELECT 1 INTO n; RETURN FOUND; END;"));
    assert(value == "f" && !isNull);
    result = {};
    result.sqlState = "42P01";
    result.message = "relation missing";
    assert(!run("DECLARE n INT; BEGIN SELECT id INTO n FROM missing; RETURN n; END;"));
    assert(state == "42P01" && error == "relation missing");
    const int beforeUnknown = calls;
    assert(!run("BEGIN SELECT 1 INTO missing; END;"));
    assert(state == "42601" && calls == beforeUnknown);
    result = rows(1, {"1"});
    result.columnCount = 2;
    assert(!run("DECLARE n INT; BEGIN SELECT 1 INTO n; END;"));
    assert(state == "XX000");  // inconsistent host carrier, not relaxed SQL width

    // Lexical protection: INTO/semicolon tokens in values and nested
    // comments never become procedure delimiters. Delimited variable names
    // preserve case, spaces, and doubled identifier quotes.
    result = rows(1, {"literal"});
    assert(run("DECLARE n TEXT; BEGIN SELECT 'INTO; ''quoted''' INTO n; RETURN n; END;"));
    assert(seen == "SELECT 'INTO; ''quoted'''");
    assert(run("DECLARE n TEXT; BEGIN SELECT $tag$INTO n; O'Reilly$tag$ INTO n; RETURN n; END;"));
    assert(seen == "SELECT $tag$INTO n; O'Reilly$tag$");
    assert(run("DECLARE n TEXT; BEGIN SELECT /* INTO bad; /* nested ; */ */ 'x' INTO n; RETURN n; END;"));
    assert(seen == "SELECT /* INTO bad; /* nested ; */ */ 'x'");
    assert(run("DECLARE n TEXT; BEGIN SELECT 'x' -- INTO bad;\n INTO n; RETURN n; END;"));
    assert(seen == "SELECT 'x' -- INTO bad;");
    assert(run("DECLARE \"Mi\"\"xed Name\" TEXT; BEGIN SELECT 'x' INTO \"Mi\"\"xed Name\"; RETURN \"Mi\"\"xed Name\"; END;"));
    assert(value == "literal" && !isNull);
    assert(run("DECLARE \"Upper\" TEXT; BEGIN \"Upper\" := 'new'; RETURN \"Upper\"; END;"));
    assert(value == "new" && !isNull);

    result = rows(1, {"1"});
    assert(run("DECLARE n INT; id INT := 99; BEGIN SELECT t.id INTO n FROM ranges t WHERE t.id=1; RETURN n; END;"));
    assert(seen == "SELECT t.id FROM ranges t WHERE t.id=1");
    assert(run("DECLARE n INT; id INT := 99; BEGIN SELECT t . id INTO n FROM ranges t; RETURN n; END;"));
    assert(seen == "SELECT t . id FROM ranges t");
    assert(run("DECLARE n TEXT; p TEXT := '00123'; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('00123' AS TEXT)");
    assert(run("DECLARE n TEXT; p TEXT := '1.000'; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('1.000' AS TEXT)");
    assert(run("DECLARE n TEXT; p TEXT := ''; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('' AS TEXT)");
    assert(run("DECLARE n TEXT; p TEXT := 'null'; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('null' AS TEXT)");
    assert(run("DECLARE n TEXT; p TEXT := NULL; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST(NULL AS TEXT)");
    assert(run("DECLARE n TEXT; p TEXT := 'O''Reilly'; BEGIN SELECT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('O''Reilly' AS TEXT)");
    assert(run("DECLARE n TEXT; E INT := 99; BEGIN SELECT E'INTO; escaped\\\'quote' INTO n; RETURN n; END;"));
    assert(seen == "SELECT E'INTO; escaped\\\'quote'");
    assert(run("DECLARE n INT; ranges INT := 99; t INT := 88; id INT := 77; BEGIN SELECT t.id INTO n FROM ranges t; RETURN n; END;"));
    assert(seen == "SELECT t.id FROM ranges t");
    assert(run("DECLARE n INT; ranges INT := 99; t INT := 88; renamed INT := 77; BEGIN SELECT t.id AS renamed INTO n FROM ranges AS t; RETURN n; END;"));
    assert(seen == "SELECT t.id AS renamed FROM ranges AS t");
    assert(run("DECLARE n INT; abs INT := 99; BEGIN SELECT abs(-7) INTO n; RETURN n; END;"));
    assert(seen == "SELECT abs(-7)");
    assert(run("DECLARE n INT; t INT := 99; BEGIN SELECT t.id INTO n FROM (SELECT 1 AS id) t; RETURN n; END;"));
    assert(seen == "SELECT t.id FROM (SELECT 1 AS id) t");
    assert(run("DECLARE n INT; t INT := 99; u INT := 88; BEGIN SELECT t.id INTO n FROM ranges t JOIN ranges u ON t.id=u.id; RETURN n; END;"));
    assert(seen == "SELECT t.id FROM ranges t JOIN ranges u ON t.id=u.id");
    assert(run("DECLARE n INT; t INT := 99; u INT := 88; BEGIN SELECT t.id INTO n FROM ranges t, ranges u; RETURN n; END;"));
    assert(seen == "SELECT t.id FROM ranges t, ranges u");
    assert(run("DECLARE n INT; c INT := 99; d INT := 88; id INT := 77; BEGIN WITH c(id) AS (SELECT 1), d(id) AS (SELECT 2) SELECT c.id INTO n FROM c; RETURN n; END;"));
    assert(seen == "WITH c(id) AS (SELECT 1), d(id) AS (SELECT 2) SELECT c.id FROM c");
    assert(run("DECLARE n INT; id INT := 99; BEGIN SELECT a.id INTO n FROM ranges a JOIN ranges b USING(id); RETURN n; END;"));
    assert(seen == "SELECT a.id FROM ranges a JOIN ranges b USING(id)");
    assert(run("DECLARE n INT; id INT := 99; v INT := 88; BEGIN SELECT a.id INTO n FROM ranges a JOIN ranges b USING(id,v) WHERE a.id=v; RETURN n; END;"));
    assert(seen == "SELECT a.id FROM ranges a JOIN ranges b USING(id,v) WHERE a.id=CAST('88' AS INT)");
    assert(run("DECLARE n TEXT; text INT := 99; p INT := 1; BEGIN SELECT p::text INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('1' AS INT)::text");
    assert(run("DECLARE n TEXT; precision INT := 99; zone INT := 88; p INT := 1; BEGIN SELECT p::double precision,CAST(p AS timestamp without time zone) INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('1' AS INT)::double precision,CAST(CAST('1' AS INT) AS timestamp without time zone)");
    assert(run("DECLARE n INT; x INT := 99; BEGIN SELECT 1 x INTO n; RETURN n; END;"));
    assert(seen == "SELECT 1 x");
    assert(run("DECLARE n INT; p INT := 7; x INT := 99; BEGIN SELECT (p+1) x INTO n; RETURN n; END;"));
    assert(seen == "SELECT (CAST('7' AS INT)+1) x");
    assert(run("DECLARE n INT; p INT := 7; x INT := 99; BEGIN SELECT p+x INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('7' AS INT)+CAST('99' AS INT)");
    assert(run("DECLARE n INT; p INT := 7; x INT := 99; BEGIN SELECT p,x INTO n; RETURN n; END;"));
    assert(seen == "SELECT CAST('7' AS INT),CAST('99' AS INT)");
    assert(run("DECLARE n INT; p INT := 7; BEGIN SELECT DISTINCT p INTO n; RETURN n; END;"));
    assert(seen == "SELECT DISTINCT CAST('7' AS INT)");
    assert(run("DECLARE n INT; p INT := 7; BEGIN SELECT ALL p INTO n; RETURN n; END;"));
    assert(seen == "SELECT ALL CAST('7' AS INT)");
    assert(run("DECLARE n INT; p INT := 7; BEGIN SELECT DISTINCT ON(p) p INTO n; RETURN n; END;"));
    assert(seen == "SELECT DISTINCT ON(CAST('7' AS INT)) CAST('7' AS INT)");
    assert(run("DECLARE n INT; materialized INT := 99; BEGIN WITH c AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id INTO n FROM c; RETURN n; END;"));
    assert(seen == "WITH c AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id FROM c");

    // Scanner changes also retain the interpreter's standalone control-flow
    // and arithmetic behavior without needing a storage-engine host.
    assert(run("BEGIN x := 1 + 2; RETURN x * 10; END;"));
    assert(value == "30" && !isNull);
    assert(run("DECLARE i INT := 0; s INT := 0; BEGIN WHILE i < 5 LOOP s := s + i; i := i + 1; END LOOP; RETURN s; END;"));
    assert(value == "10" && !isNull);
    assert(run("DECLARE s INT := 0; BEGIN FOR i IN REVERSE 4..1 LOOP s := s + i; END LOOP; RETURN s; END;"));
    assert(value == "10" && !isNull);
    assert(run("BEGIN IF 1 > 2 THEN RETURN 'a'; ELSIF 2 > 1 THEN RETURN 'b'; ELSE RETURN 'c'; END IF; END;"));
    assert(value == "b" && !isNull);

    // FOR iterators are loop-local, including their NULL bitmap. The
    // iterator shadows an outer binding and restores it on nested-loop exit.
    assert(run("DECLARE i INT; BEGIN FOR i IN 1..2 LOOP RETURN i; END LOOP; END;"));
    assert(value == "1" && !isNull);
    assert(run("DECLARE i INT := NULL; BEGIN FOR i IN REVERSE 2..1 LOOP RETURN i; END LOOP; END;"));
    assert(value == "2" && !isNull);
    assert(run("DECLARE i INT; s INT := 0; BEGIN FOR i IN 1..2 LOOP s := s + i; END LOOP; RETURN i; END;"));
    assert(isNull);
    assert(run("DECLARE i INT := 42; BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN i; END;"));
    assert(value == "42" && !isNull);
    assert(run("DECLARE i INT := 42; s INT := 0; BEGIN FOR i IN 1..2 LOOP FOR i IN 7..8 LOOP s := s + i; END LOOP; s := s + i; END LOOP; RETURN s; END;"));
    assert(value == "33" && !isNull);
    assert(run("DECLARE i INT := NULL; BEGIN FOR i IN 2..1 LOOP RETURN i; END LOOP; RETURN i; END;"));
    assert(isNull);
    assert(run("DECLARE i INT := 42; BEGIN FOR i IN REVERSE 1..2 LOOP RETURN i; END LOOP; RETURN i; END;"));
    assert(value == "42" && !isNull);

    // Typed scalar fallback retains SQL NULL through expression use, not
    // merely direct RETURN variable. It wins over the legacy string host.
    result = rows(1, {std::nullopt});
    std::string expression;
    Result scalar = rows(1, {std::nullopt});
    scalar.columnTypes = {"text"};
    int scalarCalls = 0, legacyCalls = 0;
    std::map<std::string, std::string> scalarVars, scalarTypes;
    std::set<std::string> scalarNulls;
    host.evalExpr = [&](const std::string&, const std::map<std::string, std::string>&) {
        ++legacyCalls;
        return std::optional<std::string>{"legacy"};
    };
    host.evalExprTyped = [&](const std::string& sql,
                             const std::map<std::string, std::string>& variables,
                             const std::set<std::string>& nulls,
                             const std::map<std::string, std::string>& types) {
        expression = sql;
        scalarVars = variables;
        scalarTypes = types;
        scalarNulls = nulls;
        ++scalarCalls;
        return scalar;
    };
    assert(run("DECLARE v TEXT; BEGIN SELECT NULL INTO v; RETURN v || 'x'; END;"));
    assert(expression == "v || 'x'" && isNull && scalarCalls == 1 && !legacyCalls);
    assert(scalarNulls.count("v") && scalarTypes.at("v") == "TEXT");
    assert(run("DECLARE v TEXT; BEGIN SELECT NULL INTO v; RETURN CAST(v AS TEXT); END;"));
    assert(expression == "CAST(v AS TEXT)" && isNull && !legacyCalls);
    assert(run("DECLARE v TEXT; text INT := 99; BEGIN SELECT NULL INTO v; RETURN v::text; END;"));
    assert(expression == "v::text" && isNull && scalarVars.at("text") == "99");
    assert(run("DECLARE v TEXT; BEGIN SELECT NULL INTO v; RETURN (v); END;"));
    assert(expression == "(v)" && isNull);
    assert(run("BEGIN RETURN 'a' || 'b'; END;"));
    assert(expression == "'a' || 'b'" && isNull);  // not misparsed as one literal
    for (const std::string datum : {"null", "", "O'Reilly", "00123"}) {
        scalar = rows(1, {datum});
        scalar.columnTypes = {"text"};
        assert(run("BEGIN RETURN 'a' || 'b'; END;"));
        assert(value == datum && !isNull);
    }
    scalar = {};
    scalar.sqlState = "22012";
    scalar.message = "division by zero";
    assert(!run("BEGIN RETURN 1/0; END;"));
    assert(state == "22012" && error == "division by zero");
    assert(!run("DECLARE n INT := 1/0; BEGIN RETURN n; END;"));
    assert(state == "22012" && error == "division by zero");
    assert(!run("BEGIN IF 1/0>0 THEN RETURN 1; END IF; RETURN 2; END;"));
    assert(state == "22012" && error == "division by zero");
    assert(!run("BEGIN WHILE 1/0>0 LOOP RETURN 1; END LOOP; RETURN 2; END;"));
    assert(state == "22012" && error == "division by zero");
    scalar = rows(1, {"bad"});
    scalar.columnCount = 2;
    assert(!run("BEGIN RETURN 1/0; END;"));
    assert(state == "XX000");

    // Real SQL types are a separate binding channel. The host receives raw
    // expressions, numeric variables remain numeric, and text stays text.
    scalar = rows(1, {"1"});
    scalar.columnTypes = {"integer"};
    assert(run("DECLARE s INT := 0; i INT := 1; BEGIN RETURN s+i; END;"));
    assert(expression == "s+i" && scalarVars.at("s") == "0" && scalarVars.at("i") == "1");
    assert(scalarTypes.at("s") == "INT" && scalarTypes.at("i") == "INT");
    assert(value == "1" && !isNull && returnType == "integer");
    assert(run("BEGIN RETURN 1.9; END;"));
    assert(value == "1.9" && returnType == "numeric");
    assert(run("BEGIN RETURN '1.9'; END;"));
    assert(value == "1.9" && returnType == "text");

    // Coercion is host-owned but must occur at every local assignment
    // boundary with the actual source type, including procedural INTO.
    std::vector<std::string> conversions;
    host.coerceValueTyped = [&](const std::optional<std::string>& input,
                                const std::string& source,
                                const std::string& target) {
        conversions.push_back(source + "->" + target);
        Result converted = rows(1, {input});
        converted.columnTypes = {target};
        if (input && (target == "INT" || target == "integer")) {
            if (source == "numeric") {
                converted.firstRow = {std::to_string(static_cast<long long>(std::round(std::stod(*input))))};
            } else if (input->empty() || input->find_first_not_of("+-0123456789") != std::string::npos) {
                converted = {};
                converted.sqlState = "22P02";
                converted.message = "invalid input syntax for integer";
            }
        }
        return converted;
    };
    assert(run("DECLARE n INT DEFAULT 1.9; BEGIN RETURN n; END;"));
    assert(value == "2" && !isNull && returnType == "INT");
    assert(conversions.back() == "numeric->INT");
    assert(!run("DECLARE n INT := '1.9'; BEGIN RETURN n; END;"));
    assert(state == "22P02" && conversions.back() == "text->INT");
    assert(run("DECLARE n INT; BEGIN n := 1.9; RETURN n; END;"));
    assert(value == "2" && conversions.back() == "numeric->INT");
    assert(!run("DECLARE n INT; BEGIN n := '1.9'; RETURN n; END;"));
    assert(state == "22P02" && conversions.back() == "text->INT");
    result = rows(1, {"1.9"});
    result.columnTypes = {"numeric"};
    assert(run("DECLARE n INT; BEGIN SELECT 1.9 INTO n; RETURN n; END;"));
    assert(value == "2" && conversions.back() == "numeric->INT");
    result.columnTypes = {"text"};
    assert(!run("DECLARE n INT; BEGIN SELECT '1.9' INTO n; RETURN n; END;"));
    assert(state == "22P02" && conversions.back() == "text->INT");
    result = rows(0, {"unused"});
    result.columnTypes = {"numeric"};
    assert(run("DECLARE n INT DEFAULT 7; BEGIN SELECT 1.9 INTO n WHERE FALSE; RETURN n; END;"));
    assert(isNull && conversions.back() == "numeric->INT");
    result = rows(1, {"7"});
    result.columnTypes = {"integer"};
    assert(run("DECLARE n INT; v TEXT; BEGIN SELECT 7 INTO n,v; RETURN v; END;"));
    assert(isNull && conversions.back() == "unknown->TEXT");
    assert(run("DECLARE n TEXT; p TEXT DEFAULT '00123'; BEGIN SELECT p INTO n; RETURN p; END;"));
    assert(seen == "SELECT CAST('00123' AS TEXT)" && value == "00123" && returnType == "TEXT");
    assert(run("DECLARE n TEXT; p TEXT DEFAULT 'default; INTO'; BEGIN SELECT p INTO n; RETURN p; END;"));
    assert(seen == "SELECT CAST('default; INTO' AS TEXT)");
    assert(run("DECLARE i TEXT DEFAULT 'outside'; n INT; BEGIN FOR i IN 1..1 LOOP SELECT i INTO n; END LOOP; RETURN i; END;"));
    assert(seen == "SELECT CAST('1' AS integer)" && value == "outside" && returnType == "TEXT");

    host.parameterTypes = {{"arg", "integer"}, {"payload", "text"}};
    assert(dbms::PlPgsql::run("BEGIN RETURN arg+1; END;",
        {{"arg", "2"}, {"payload", "00123"}}, host, value, error, nullptr,
        &isNull, nullptr, &state, &returnType));
    assert(expression == "arg+1" && scalarTypes.at("arg") == "integer" &&
           scalarTypes.at("payload") == "text" && scalarVars.at("payload") == "00123");
    host.coerceValueTyped = {};
    host.evalExprTyped = {};
    assert(run("BEGIN RETURN abs(-7); END;"));
    assert(value == "legacy" && !isNull && legacyCalls == 1);

    std::cout << "[PLPGSQL SELECT INTO] passed\n";
}
