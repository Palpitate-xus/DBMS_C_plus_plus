#include "utils/plpgsql.h"
#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>

using namespace dbms;

static PlPgsqlHost typedHost() {
    PlPgsqlHost host;
    host.evalExprTyped = [](const std::string& text, const auto& vars,
                            const auto& nulls, const auto& types) {
        const auto value = ExprHelper::evalStringWithNulls(text, vars, nulls, types);
        PlPgsqlQueryResult result;
        result.ok = value.ok; result.sqlState = value.sqlState; result.message = value.error;
        if (value.ok) {
            result.rowCount = result.columnCount = 1;
            result.columnTypes = {value.typeName};
            result.firstRow = {value.isNull ? std::nullopt : std::optional<std::string>{value.value}};
        }
        return result;
    };
    return host;
}

static void expectError(const std::string& body, const std::string& state,
                        const std::string& contains = {}) {
    std::string value, error, actual;
    size_t notices = 0;
    const bool ok = PlPgsql::run(body, {}, typedHost(), value, error,
        [&](const auto&, const auto&) { ++notices; }, nullptr, nullptr, &actual);
    if (ok || actual != state || error.find(contains) == std::string::npos) {
        std::cerr << body << "\nactual ok=" << ok << " state=" << actual
                  << " message=" << error << " expected=" << state << '\n';
    }
    assert(!ok && actual == state && error.find(contains) != std::string::npos);
    assert(notices == 0); // errors are not also nonfatal notice messages
}

int main() {
    expectError("BEGIN RAISE EXCEPTION 'late failure'; END;", "P0001", "late failure");
    expectError("BEGIN RAISE 'default failure'; END;", "P0001", "default failure");
    expectError("BEGIN RAISE ERROR 'legacy error alias'; END;", "P0001");
    expectError("DECLARE code TEXT := '22012'; BEGIN RAISE 'custom' USING ERRCODE=code; END;", "22012");
    expectError("BEGIN RAISE 'named' USING ERRCODE='unique_violation'; END;", "23505");
    expectError("BEGIN RAISE 'numeric' USING ERRCODE=23505; END;", "23505");
    expectError("BEGIN RAISE 'default zero' USING ERRCODE='00000'; END;", "P0001");
    expectError("BEGIN RAISE SQLSTATE 'P1234'; END;", "P1234", "P1234");
    expectError("BEGIN RAISE division_by_zero; END;", "22012", "division_by_zero");
    expectError("BEGIN RAISE null_value_not_allowed; END;", "22004");
    expectError("BEGIN RAISE USING ERRCODE := '22023', MESSAGE := 'from option'; END;", "22023", "from option");
    expectError("BEGIN RAISE 'x' USING ERRCODE=NULL; END;", "22004");
    expectError("BEGIN RAISE USING MESSAGE=NULL; END;", "22004");
    expectError("BEGIN RAISE 'x' USING ERRCODE='p0001'; END;", "42704");
    expectError("BEGIN RAISE 'x' USING ERRCODE='UNIQUE_VIOLATION'; END;", "42704");
    expectError("BEGIN RAISE nonexistent_condition; END;", "42704");
    expectError("BEGIN RAISE SQLSTATE 'p0001'; END;", "42601");
    expectError("BEGIN RAISE; END;", "0Z002");
    expectError("BEGIN RAISE NOTICE; END;", "42601");
    expectError("BEGIN RAISE '%/%%/%/%', NULL, 'null', 'O''Brien'; END;",
                "P0001", "<NULL>/%/null/O'Brien");
    expectError("BEGIN RAISE 'x' USING ERRCODE='23505', ERRCODE='22012'; END;", "42601");
    expectError("BEGIN RAISE 'x' USING MESSAGE='other'; END;", "42601");
    expectError("BEGIN RAISE division_by_zero USING ERRCODE='23505'; END;", "42601");
    expectError("BEGIN RAISE '%', 1/0; END;", "22012");
    expectError("BEGIN RAISE 'x' USING ERRCODE=CAST('bad' AS INT); END;", "22P02");
    expectError("BEGIN RAISE 'x' USING ERRCODE=; END;", "42601");
    expectError("BEGIN RAISE '%'; END;", "42601");
    expectError("BEGIN RAISE 'no args', 1; END;", "42601");
    expectError("BEGIN RAISE 'x' USING BOGUS='ignored'; END;", "42601");
    // Compile-time format checks must reject the entire body before any
    // preceding host statement runs (including nontransactional effects).
    auto host = typedHost(); size_t effects = 0;
    host.execStmt = [&](const auto&, const auto&) { ++effects; return true; };
    std::string value, error, state;
    assert(!PlPgsql::run("BEGIN PERFORM 1; RAISE '%'; END;", {}, host,
                        value, error, nullptr, nullptr, nullptr, &state));
    assert(state == "42601" && effects == 0);
    std::vector<std::string> notices;
    assert(PlPgsql::run("BEGIN RAISE NOTICE 'using,%/%%/%', 1+2, 'O''Brien'; "
        "RAISE WARNING USING MESSAGE='warning'; RETURN 'ok'; END;", {}, host,
        value, error, [&](const auto& level, const auto& message) {
            notices.push_back(level + ":" + message);
        }, nullptr, nullptr, &state));
    assert(value == "ok" && state.empty());
    assert((notices == std::vector<std::string>{"notice:using,3/%/O'Brien", "warning:warning"}));
    expectError("BEGIN RAISE E'escaped\\n%%'; END;", "P0001", "escaped\n%");
    expectError("BEGIN RAISE $fmt$dollar,using %%$fmt$; END;", "P0001", "dollar,using %");
    std::cout << "[PLPGSQL RAISE SQLSTATE] passed\n";
}
