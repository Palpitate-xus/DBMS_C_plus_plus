#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <iostream>
#include <regex>

extern dbms::StorageEngine g_engine;

namespace {
struct Input {std::string sql, type; std::optional<std::string> value;};
struct Target {std::optional<bool> value; std::string state;};
const std::map<std::string, std::pair<int64_t, int64_t>> limits = {
    {"smallint", {-32768, 32767}}, {"integer", {-2147483648LL, 2147483647LL}},
    {"bigint", {INT64_MIN, INT64_MAX}}
};
std::optional<std::string> integer(std::optional<std::string> input, const std::string& type) {
    if (!input) return {};
    const auto begin = input->find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) throw std::string("22P02");
    *input = input->substr(begin, input->find_last_not_of(" \t\r\n") - begin + 1);
    if (!std::regex_match(*input, std::regex("[+-]?[0-9]+"))) throw std::string("22P02");
    int64_t value;
    try {value = std::stoll(*input);} catch (const std::out_of_range&) {throw std::string("22003");}
    if (value < limits.at(type).first || value > limits.at(type).second) throw std::string("22003");
    return std::to_string(value);
}
std::optional<bool> compare(Input left, Input right, bool lower) {
    if (left.type == "unknown" && right.type == "unknown") left.type = right.type = "text";
    else if (left.type == "unknown") {left.type = right.type; if (limits.count(left.type)) left.value = integer(left.value, left.type);}
    else if (right.type == "unknown") {right.type = left.type; if (limits.count(right.type)) right.value = integer(right.value, right.type);}
    if (bool(limits.count(left.type)) != bool(limits.count(right.type))) throw std::string("42883");
    if (!left.value || !right.value) return {};
    if (limits.count(left.type)) return lower ? std::stoll(*left.value) >= std::stoll(*right.value) : std::stoll(*left.value) <= std::stoll(*right.value);
    return lower ? *left.value >= *right.value : *left.value <= *right.value;
}
Target expected(const Input& left, const Input& lower, const Input& upper, bool negate) {
    try {
        const auto a = compare(left, lower, true), b = compare(left, upper, false);
        if ((a && !*a) || (b && !*b)) return {negate, ""};
        if (!a || !b) return {{}, ""};
        return {!negate, ""};
    } catch (const std::string& state) {return {{}, state};}
}
std::vector<Input> bounds(const std::string& type) {
    return {{"0::" + type, type, "0"}, {"2::" + type, type, "2"}, {"NULL::" + type, type, {}},
        {"'1'", "unknown", "1"}, {"'b01'", "unknown", "b01"}, {"''", "unknown", ""},
        {"NULL", "unknown", {}}, {"'1'::text", "text", "1"}};
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("integer_between_input");
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;
    size_t controls = 0, failures = 0, scalarCases = 0, helperCases = 0, cursorCases = 0, projectionCases = 0;
    const auto require = [&](bool valid, const std::string& label) {
        ++controls; if (!valid) {++failures; std::cerr << "[INTEGER BETWEEN FAIL] " << label << '\n';}
    };
    const auto cursor = [&](const std::string& expression, const Target& target,
                            const std::vector<QueryBindingDatum>& parameters = std::vector<QueryBindingDatum>{}) {
        ++cursorCases; std::string state; bool valid = false;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, "SELECT " + expression + " AS value", parameters));
            auto rows = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                &g_engine, database, prepared, prepared->ast.get()), prepared->output);
            std::vector<ExprValue> row;
            valid = prepared->output.size() == 1 && prepared->output[0].type == "boolean" && rows->next(row) && row.size() == 1 &&
                row[0].typeName == "boolean" && row[0].isNull == !target.value && (!target.value || row[0].asBool() == *target.value) && !rows->next(row);
            rows->close();
        } catch (const DbError& error) {state = error.sqlState();}
        catch (const std::exception& error) {state = "UNSTRUCTURED"; std::cerr << error.what() << '\n';}
        require(state == target.state && (!state.empty() || valid), "actual prepared cursor " + expression + " state=" + state);
    };
    const auto helper = [&](const std::string& expression, const Target& target) {
        ++helperCases; const auto result = ExprHelper::evalString(expression, {});
        require(result.sqlState == target.state && result.ok == target.state.empty() &&
            (!result.ok || (result.typeName == "boolean" && result.isNull == !target.value &&
             (!target.value || ExprValue("boolean", result.value).asBool() == *target.value))), "actual helper " + expression + " state=" + result.sqlState);
    };
    for (const auto& width : limits) {
        const auto& type = width.first;
        const std::vector<Input> lefts = {{"'1'", "unknown", "1"}, {"NULL", "unknown", {}}, {"'b01'", "unknown", "b01"},
            {"''", "unknown", ""}, {"'32768'", "unknown", "32768"}, {"'2147483648'", "unknown", "2147483648"},
            {"'9223372036854775808'", "unknown", "9223372036854775808"}, {"' +01 '", "unknown", " +01 "},
            {"1::" + type, type, "1"}, {"NULL::" + type, type, {}}, {"'1'::text", "text", "1"}, {"NULL::text", "text", {}}};
        for (const auto& left : lefts) for (const auto& lower : bounds(type)) for (const auto& upper : bounds(type))
            for (const bool negate : {false, true}) {
                ++scalarCases; const auto target = expected(left, lower, upper, negate);
                const auto expression = left.sql + (negate ? " NOT BETWEEN " : " BETWEEN ") + lower.sql + " AND " + upper.sql;
                helper(expression, target); cursor(expression, target);
            }
    }
    require(scalarCases == 4608 && helperCases == 4608 && cursorCases == 4608, "all full 4608 scalar cases reach both actual owners");
    size_t parameterCases = 0;
    for (const auto& source : std::vector<std::optional<std::string>>{"01", "b01", "2", "", {}, "32768", "2147483648"}) {
        for (const auto& type : {"unknown", "text"}) {
            const std::vector<QueryBindingDatum> parameters = {{"owned-integer-range-parameter", "p", type, {}, true, source, 1}};
            for (const auto& range : {"1 AND 2", "NULL AND 1", "'1' AND 1", "1 AND NULL"}) {
                ++parameterCases;
                Target target;
                if (std::string(type) == "text" || std::string(range) == "NULL AND 1" || std::string(range) == "'1' AND 1")
                    target.state = "42883";
                else {
                    try {
                        const auto parsed = integer(source, "integer");
                        if (parsed) {
                            const auto value = std::stoll(*parsed);
                            if (std::string(range) == "1 AND 2") target.value = value >= 1 && value <= 2;
                            else if (value < 1) target.value = false;
                        }
                    } catch (const std::string& state) {target.state = state;}
                }
                cursor(std::string("$1 BETWEEN ") + range, target, parameters);
            }
        }
        for (const auto& kind : {"smallint", "bigint"}) {
            ++parameterCases; Target target;
            try {const auto value = integer(source, kind); if (value) target.value = std::stoll(*value) >= 0 && std::stoll(*value) <= 2;}
            catch (const std::string& state) {target.state = state;}
            cursor(std::string("$1::") + kind + " BETWEEN 0 AND 2", target,
                {{"owned-integer-range-cast-parameter", "p", "unknown", {}, true, source, 1}});
        }
    }
    require(parameterCases == 70, "all real shared UNKNOWN/TEXT/explicit width parameter cells reached");
    TableSchema calls; calls.tablename = "calls"; calls.append(makeIntColumn("id", false, 4));
    if (g_engine.createTable(database, calls) != DBStatus::OK || g_engine.createUDF(database, "integer_range_writer", {"p"}, {"integer"},
        "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;", 'v', "plpgsql", "integer") != DBStatus::OK) return 2;
    const auto count = [&] {return g_engine.query(database, "calls", {}, {"id"}, {}).size();};
    for (const auto& argument : {"1", "NULL::integer"}) for (const auto& range : {"'b01' AND 2", "0 AND 'b01'", "NULL AND 'b01'", "'b01' AND NULL", "'' AND 2", "0 AND ''"})
        for (const auto& operation : {" BETWEEN ", " NOT BETWEEN "}) {
            const auto expression = std::string("integer_range_writer(") + argument + ")" + operation + range;
            std::string state; const auto before = count();
            try {(void)g_engine.prepareBoundQuery(database, "SELECT " + expression);}
            catch (const DbError& error) {state = error.sqlState();}
            require(state == "22P02" && count() == before, "real binder pure literal transformation never executes writer " + expression);
            cursor(expression, {{}, "22P02"});
            const auto result = ExprHelper::evalString(expression, {}, {}, database, "admin", &g_engine);
            require(!result.ok && result.sqlState == "22P02" && count() == before, "real helper input admission never executes writer " + expression);
        }
    for (const auto& width : limits) for (const auto& population : {"populated", "empty", "nulls"}) {
        const auto& type = width.first; TableSchema table; table.tablename = type + "_" + population;
        table.append(makeIntColumn("id", false, 4, true)); Column column; column.dataName = "i"; column.isNull = true;
        if (!TypeRegistry::instance().resolveColumnType(column, type, {}, false).empty()) return 2;
        table.append(column);
        if (g_engine.createTable(database, table) != DBStatus::OK) return 2;
        if (std::string(population) != "empty" && g_engine.insertRow(database, table.tablename,
            {{"id", "1"}, {"i", std::string(population) == "nulls" ? std::nullopt : std::optional<std::string>("1")}}) != DBStatus::OK) return 2;
        const Input left{"i", type, std::string(population) == "nulls" ? std::nullopt : std::optional<std::string>("1")};
        const auto members = bounds(type);
        for (const auto& indexes : std::vector<std::pair<size_t, size_t>>{{3,1},{0,4},{4,1},{6,4},{4,6},{5,6},{6,5},{7,1}})
            for (const bool negate : {false, true}) for (const size_t cap : {size_t(0), size_t(1)}) {
                ++projectionCases; const auto target = expected(left, members[indexes.first], members[indexes.second], negate);
                const auto expression = left.sql + (negate ? " NOT BETWEEN " : " BETWEEN ") + members[indexes.first].sql + " AND " + members[indexes.second].sql;
                StorageEngine::SelectExpr projection; projection.displayName = "value"; projection.isScalar = true;
                projection.funcName = "expreval"; projection.funcArgs = {expression};
                StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
                std::string state; std::vector<std::vector<std::string>> rows; std::vector<std::vector<bool>> nulls;
                try {(void)g_engine.queryExpr(database, table.tablename, {}, {projection}, {}, &rows, &nulls, nullptr, options);}
                catch (const DbError& error) {state = error.sqlState();}
                catch (const std::exception&) {state = "UNSTRUCTURED";}
                const bool receives = cap && std::string(population) != "empty";
                const bool valid = !receives ? rows.empty() : rows.size() == 1 && rows[0].size() == 1 && nulls.size() == 1 && nulls[0].size() == 1 &&
                    nulls[0][0] == !target.value && (!target.value || rows[0][0] == (*target.value ? "t" : "f"));
                require(state == target.state && (!state.empty() || valid), "real projection pre-admission " + table.tablename + " cap=" + std::to_string(cap) + " " + expression);
            }
    }
    require(projectionCases == 288 && cursorCases == 4702, "all whole 288 empty/cap0/populated/NULL projections, 70 parameters and 24 writers reached");
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "[INTEGER BETWEEN INPUT] all " << controls << " helper/binder/cursor/width/NULL/overflow/empty/cap0/pure-writer controls; failures=" << failures << '\n';
    return failures ? 1 : 0;
}
