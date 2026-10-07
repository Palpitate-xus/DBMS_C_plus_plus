#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/query_binding.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace {
struct Operand {
    std::string sql;
    std::optional<std::string> bits;
    bool constantNull = false;
    size_t calls = 0;
};
struct Target {std::optional<std::string> value; size_t calls; std::string state; std::string type = "boolean";};
Operand literal(std::optional<std::string> bits) {
    return {bits ? "B'" + *bits + "'" : "NULL::varbit", bits, !bits, 0};
}
Operand writer(std::optional<std::string> bits) {
    return {"public.demand_writer(" + literal(bits).sql + ")", bits, false, 1};
}
Operand child(std::optional<std::string> bits) {
    return {"(SELECT " + writer(bits).sql + ")", bits, false, 1};
}
Operand bound(const std::string& sql) {
    static const std::map<std::string, std::optional<std::string>> inputs = {
        {"B'01'", "01"}, {"B'11'", "11"}, {"B'00'", "00"}, {"NULL", {}},
        {"'b01'", "01"}, {"'x1'", "0001"}, {"'102'", "INVALID"}, {"'01'", "01"}, {"'11'", "11"}
    };
    return {sql, inputs.at(sql), sql == "NULL", 0};
}
std::pair<std::optional<bool>, size_t> compare(const Operand& left, const Operand& right, bool lower) {
    if (left.constantNull || right.constantNull) return {{}, 0};
    if (!left.bits || !right.bits) return {{}, left.calls + right.calls};
    return {lower ? *left.bits >= *right.bits : *left.bits <= *right.bits, left.calls + right.calls};
}
Target expected(const Operand& left, const Operand& lower, const Operand& upper, bool negate) {
    if (lower.bits == "INVALID" || upper.bits == "INVALID") return {{}, 0, "22P02"};
    const auto a = compare(left, lower, true);
    if (a.first && !*a.first) return {negate ? "t" : "f", a.second, ""};
    const auto b = compare(left, upper, false);
    if (b.first && !*b.first) return {negate ? "t" : "f", a.second + b.second, ""};
    if (!a.first || !b.first) return {{}, a.second + b.second, ""};
    return {negate ? "f" : "t", a.second + b.second, ""};
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_between_demand");
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;
    Session session; session.username = "admin"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore {Session* previous; ~Restore() {setCurrentSession(previous);}} restore{previous};
    TableSchema sink; sink.tablename = "calls"; sink.append(makeIntColumn("id", false, 4));
    if (g_engine.createTable(database, sink) != DBStatus::OK ||
        g_engine.createSchema(database, "role_scope") != DBStatus::OK ||
        g_engine.createUDF(database, "demand_writer", {"p"}, {"varbit"},
            "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;", 'v', "plpgsql", "varbit") != DBStatus::OK) return 2;
    for (const auto& name : {"between", "not between"})
        if (g_engine.createUDF(database, name, {"a", "b", "c"}, {"varbit", "varbit", "varbit"},
            "BEGIN INSERT INTO calls VALUES(1); RETURN a; END;", 'v', "plpgsql", "varbit", false, false, "role_scope") != DBStatus::OK) return 2;
    const auto count = [&] {return g_engine.query(database, "calls", {}, {"id"}, {}).size();};
    size_t controls = 0, failures = 0, cursorCases = 0, helperCases = 0, admissionCases = 0;
    const auto require = [&](bool valid, const std::string& label) {
        ++controls; if (!valid) {++failures; std::cerr << "[BETWEEN DEMAND FAIL] " << label << '\n';}
    };
    const auto cursor = [&](const std::string& expression, const Target& target,
                            const std::vector<QueryBindingDatum>& datums = std::vector<QueryBindingDatum>{}) {
        ++cursorCases; const auto before = count(); std::string state; bool valid = false;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, "SELECT " + expression + " AS value", datums));
            require(count() == before, "genuine binder never invokes stored writer " + expression);
            auto rows = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                &g_engine, database, prepared, prepared->ast.get()), prepared->output);
            std::vector<ExprValue> row;
            valid = prepared->output.size() == 1 && prepared->output[0].type == target.type && rows->next(row) &&
                row.size() == 1 && row[0].typeName == target.type && row[0].isNull == !target.value &&
                (!target.value || row[0].value == *target.value) && !rows->next(row);
            rows->close();
        } catch (const DbError& error) {state = error.sqlState(); if (state != target.state) std::cerr << error.what() << '\n';}
        catch (const std::exception& error) {state = "UNSTRUCTURED"; std::cerr << error.what() << '\n';}
        require(state == target.state && (!state.empty() || valid) && count() - before == target.calls,
            "actual cursor " + expression + " state=" + state + " calls=" + std::to_string(count() - before));
    };
    const auto helper = [&](const std::string& expression, const Target& target) {
        ++helperCases; const auto before = count(); const auto result = ExprHelper::evalString(expression, {}, {}, database, "admin", &g_engine);
        require(result.sqlState == target.state && result.ok == target.state.empty() &&
            (!result.ok || (result.typeName == target.type && result.isNull == !target.value &&
             (!target.value || result.value == *target.value))) && count() - before == target.calls,
            "actual helper " + expression + " state=" + result.sqlState + " calls=" + std::to_string(count() - before));
    };
    const auto plannedCursor = [&](const std::string& expression, const Target& target) {
        ++cursorCases; const auto before = count(); std::string state; bool valid = false;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, "SELECT " + expression + " AS value"));
            auto execution = std::make_shared<PreparedQueryExecution>(prepared, &g_engine, database);
            execution->setChildCursorFactory([prepared, &database](const Stmt* child, const RowContext& outer) {
                return QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                    &g_engine, database, prepared, child, outer), prepared->statementOutputs.at(child));
            }, false);
            // Exercise the actual SQL host's explicit planning boundary, not
            // the raw cursor API's intentional planRootConstants=false mode.
            execution->planStatementConstants(prepared->ast.get());
            require(count() == before, "genuine pure constant planning never invokes a writer " + expression);
            auto rows = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                &g_engine, database, prepared, prepared->ast.get(), {}, [execution] {return execution;}), prepared->output);
            std::vector<ExprValue> row;
            valid = prepared->output.size() == 1 && prepared->output[0].type == "boolean" && rows->next(row) &&
                row.size() == 1 && row[0].typeName == "boolean" && row[0].isNull == !target.value &&
                (!target.value || row[0].value == *target.value) && !rows->next(row);
            rows->close();
        } catch (const DbError& error) {state = error.sqlState();}
        catch (const std::exception& error) {state = "UNSTRUCTURED"; std::cerr << error.what() << '\n';}
        require(state == target.state && (!state.empty() || valid) && count() == before,
            "actual owned planned cursor " + expression + " state=" + state + " calls=" + std::to_string(count() - before));
    };
    size_t cases = 0;
    const auto range = [&](const Operand& left, const Operand& lower, const Operand& upper, bool negate, bool helperOwner = true) {
        ++cases;
        const auto expression = left.sql + (negate ? " NOT BETWEEN " : " BETWEEN ") + lower.sql + " AND " + upper.sql;
        const auto target = expected(left, lower, upper, negate);
        cursor(expression, target); if (helperOwner) helper(expression, target);
    };
    const std::vector<std::optional<std::string>> values = {"00", "01", "11", {}};
    const std::vector<std::pair<std::string, std::string>> pairs = {
        {"B'01'", "B'11'"}, {"NULL", "B'11'"}, {"B'01'", "NULL"}, {"NULL", "NULL"},
        {"B'11'", "B'01'"}, {"'b01'", "'x1'"}, {"'102'", "B'11'"}, {"B'01'", "'102'"}};
    for (const auto& value : values) for (const auto& pair : pairs) for (const bool negate : {false, true})
        range(writer(value), bound(pair.first), bound(pair.second), negate);
    for (const auto& value : values) for (const auto& lower : {"B'01'", "NULL"})
        for (const auto& upper : std::vector<std::optional<std::string>>{"11", {}}) for (const bool negate : {false, true})
            range(literal(value), bound(lower), writer(upper), negate);
    for (const auto& value : values) for (const auto& lower : std::vector<std::optional<std::string>>{"01", {}})
        for (const auto& upper : std::vector<std::optional<std::string>>{"11", {}}) for (const bool negate : {false, true})
            range(writer(value), writer(lower), writer(upper), negate);
    require(cases == 128, "all original demand 128 SQL cases reach both real owners");
    session.searchPath = "role_scope, pg_catalog, public";
    for (const auto& name : {"between", "not between"}) for (const auto& qualifier : {"", "role_scope.", "\"role_scope\"."}) {
        const std::string function = std::string(qualifier) + '"' + name + '"';
        for (const auto& item : std::vector<std::pair<std::string, Target>>{
            {function + "(B'01',B'11',B'00')", {"01", 1, "", "bit varying"}},
            {function + "(NULL::varbit,B'11',B'00')", {{}, 1, "", "bit varying"}},
            {function + "(B'01',B'11',B'00') BETWEEN B'01' AND B'01'", {"t", 2, ""}},
            {function + "(B'00',B'11',B'00') NOT BETWEEN B'01' AND B'11'", {"t", 1, ""}}}) {
            cursor(item.first, item.second); helper(item.first, item.second);
        }
    }
    session.searchPath = "public, pg_catalog";
    range(child("01"), bound("B'01'"), bound("B'01'"), false, false);
    range(child("00"), bound("B'01'"), bound("B'11'"), false, false);
    range(child({}), bound("B'01'"), bound("B'11'"), false, false);
    range(literal("00"), bound("B'01'"), child("11"), false, false);
    range(literal("01"), bound("B'01'"), child("11"), false, false);
    range(literal("00"), bound("B'01'"), child("11"), true, false);
    range(child("01"), bound("'01'"), bound("'01'"), false, false);
    range(child("00"), bound("'01'"), bound("'11'"), false, false);
    range(child({}), bound("'01'"), bound("'11'"), false, false);
    range(child("01"), bound("'102'"), bound("B'11'"), false, false);
    range(child("01"), bound("B'00'"), bound("'102'"), false, false);
    range(literal("01"), child("00"), child("11"), false, false);
    require(cases == 140, "all 12 actual scalar child SQL cases reached real prepared owner");
    for (const auto& item : std::vector<std::pair<std::string, Target>>{
        {"CAST('2' AS bit) BETWEEN NULL AND NULL", {{}, 0, "22P02"}},
        {"CAST('2' AS varbit) BETWEEN NULL AND NULL", {{}, 0, "22P02"}},
        {"CAST('xg' AS varbit) BETWEEN NULL AND B'11'", {{}, 0, "22P02"}},
        {"NULL::integer BETWEEN 1/0 AND 1", {{}, 0, "22012"}},
        {"NULL::integer BETWEEN 1 AND 1/0", {{}, 0, "22012"}},
        {"1 BETWEEN 0 AND 'x'::integer", {{}, 0, "22P02"}},
        {"0 BETWEEN 1 AND 'x'::integer", {{}, 0, "22P02"}},
        {"0 BETWEEN 1 AND 1/0", {"f", 0, ""}},
        {"B'00' BETWEEN B'01' AND '2'::varbit", {{}, 0, "22P02"}},
        {"NULL::bit BETWEEN 1 AND NULL", {{}, 0, "42883"}},
        {"NULL::bit NOT BETWEEN NULL AND 1", {{}, 0, "42883"}},
        {"NULL::bit BETWEEN '102' AND NULL", {{}, 0, "22P02"}},
        {"NULL::bit BETWEEN NULL AND '102'", {{}, 0, "22P02"}}}) {cursor(item.first, item.second); helper(item.first, item.second);}
    for (const auto& source : std::vector<std::optional<std::string>>{"01", "b01", "2", "", {}}) {
        const auto bits = source && *source == "b01" ? std::optional<std::string>("01") : source;
        const std::string state = source == "2" ? "22P02" : "";
        const auto result = !bits ? std::optional<std::string>{} : std::optional<std::string>(*bits >= "01" && *bits <= "11" ? "t" : "f");
        // Native typed cells contain the already decoded datum. The paired
        // protocol fixture separately sends raw b01 through real Bind input.
        const std::vector<QueryBindingDatum> bindings = {{"owned-demand-parameter", "p", "bit varying", {}, true, bits, 1}};
        cursor("public.demand_writer($1::varbit) BETWEEN B'01' AND B'11'",
            {result, !state.empty() ? 0 : bits && *bits < "01" ? 1u : 2u, state}, bindings);
        cursor("$1::varbit BETWEEN public.demand_writer(B'01') AND public.demand_writer(B'11')",
            {result, !state.empty() || !bits ? 0 : *bits < "01" ? 1u : 2u, state}, bindings);
        cursor("public.demand_writer($1::varbit) BETWEEN NULL AND B'11'", {{}, state.empty() ? 1u : 0u, state}, bindings);
        cursor("$1::varbit BETWEEN NULL AND public.demand_writer(B'11')", {{}, state.empty() && bits ? 1u : 0u, state}, bindings);
    }
    const std::string zero = "(1/0)::bit";
    for (const auto& item : std::vector<std::pair<std::string, Target>>{
        {writer("00").sql + " BETWEEN B'01' AND " + zero, {{}, 0, "22012"}},
        {writer("01").sql + " BETWEEN NULL AND " + zero, {{}, 0, "22012"}},
        {writer("01").sql + " BETWEEN B'01' AND " + zero, {{}, 0, "22012"}},
        {"B'00' BETWEEN B'01' AND " + zero, {"f", 0, ""}},
        {"NULL::bit BETWEEN B'01' AND " + zero, {{}, 0, "22012"}},
        {"NULL::bit BETWEEN " + zero + " AND B'11'", {{}, 0, "22012"}},
        {"B'00' BETWEEN B'01' AND public.demand_writer(" + zero + ")", {"f", 0, ""}},
        {writer("00").sql + " BETWEEN B'01' AND public.demand_writer(" + zero + ")", {{}, 0, "22012"}},
        {"public.demand_writer(" + zero + ") BETWEEN NULL AND NULL", {{}, 0, "22012"}},
        {writer("00").sql + " BETWEEN B'01' AND (SELECT " + zero + ")", {{}, 0, "22012"}},
        {"B'00' BETWEEN B'01' AND (SELECT " + zero + ")", {"f", 0, ""}},
        {writer("00").sql + " NOT BETWEEN B'01' AND " + zero, {{}, 0, "22012"}},
        {"B'00' NOT BETWEEN B'01' AND " + zero, {"t", 0, ""}}}) plannedCursor(item.first, item.second);
    for (const auto& population : {"populated", "empty", "nulls"}) {
        TableSchema table; table.tablename = std::string("demand_") + population;
        table.append(makeIntColumn("id", false, 4, true));
        Column bit; bit.dataName = "b"; bit.isNull = true;
        if (!TypeRegistry::instance().resolveColumnType(bit, "varbit", {}, false).empty()) return 2;
        table.append(bit);
        if (g_engine.createTable(database, table) != DBStatus::OK) return 2;
        if (std::string(population) != "empty" && g_engine.insertRow(database, table.tablename,
            {{"id", "1"}, {"b", std::string(population) == "nulls" ? std::nullopt : std::optional<std::string>("01")}}) != DBStatus::OK) return 2;
        for (const size_t cap : {size_t(0), size_t(1)}) {
            for (const auto& expression : {"public.demand_writer(b) BETWEEN B'01' AND '2'::varbit",
                                          "CAST('2' AS varbit) BETWEEN NULL AND NULL",
                                          "public.demand_writer(b) BETWEEN '102' AND B'11'"}) {
                ++admissionCases; const auto before = count(); std::string state;
                StorageEngine::SelectExpr projection; projection.displayName = "value"; projection.isScalar = true;
                projection.funcName = "expreval"; projection.funcArgs = {expression};
                StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
                try {(void)g_engine.queryExpr(database, table.tablename, {}, {projection}, {}, options);}
                catch (const DbError& error) {state = error.sqlState();}
                catch (const std::exception&) {state = "UNSTRUCTURED";}
                require(state == "22P02" && count() == before, "actual native empty/cap0 input admission " + table.tablename + " " + expression);
            }
            for (const auto& expression : {"public.demand_writer(b) BETWEEN B'01' AND B'11'",
                                          "public.demand_writer(b) BETWEEN NULL AND NULL",
                                          "B'00' BETWEEN B'01' AND public.demand_writer(b)"}) {
                ++admissionCases; const auto before = count(); std::string state;
                StorageEngine::SelectExpr projection; projection.displayName = "value"; projection.isScalar = true;
                projection.funcName = "expreval"; projection.funcArgs = {expression};
                StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
                std::vector<std::vector<std::string>> rows; std::vector<std::vector<bool>> nulls;
                try {(void)g_engine.queryExpr(database, table.tablename, {}, {projection}, {}, &rows, &nulls, nullptr, options);}
                catch (const DbError& error) {state = error.sqlState();}
                catch (const std::exception&) {state = "UNSTRUCTURED";}
                const bool receives = cap && std::string(population) != "empty";
                const bool dynamic = std::string(expression) == "public.demand_writer(b) BETWEEN B'01' AND B'11'";
                const bool isNull = std::string(expression) == "public.demand_writer(b) BETWEEN NULL AND NULL" ||
                    (dynamic && std::string(population) == "nulls");
                const std::string value = dynamic ? "t" : "f";
                const bool valid = !receives ? rows.empty() : rows.size() == 1 && rows[0].size() == 1 &&
                    nulls.size() == 1 && nulls[0].size() == 1 && nulls[0][0] == isNull && (isNull || rows[0][0] == value);
                require(state.empty() && valid && count() - before == (receives && dynamic ? 2u : 0u),
                    "actual native empty/cap0/rowNULL/demand projection " + table.tablename + " " + expression);
            }
        }
    }
    require(cursorCases == 210 && helperCases == 165 && admissionCases == 36,
        "all native 197 raw cursor/13 actual planned cursor/165 helper/36 projection cases reached (no first-failure truncation)");
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "[BETWEEN DEMAND] " << controls << " complete helper/binder/cursor/128 demand/24 real routine roles/12 child/13 error controls; failures=" << failures << '\n';
    return failures ? 1 : 0;
}
