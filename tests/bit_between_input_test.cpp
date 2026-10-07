#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace {
struct Input {std::string sql, type; std::optional<std::string> value;};
std::optional<std::string> bits(std::optional<std::string> value) {
    if (!value) return {};
    if (!value->empty() && (value->front() == 'x' || value->front() == 'X')) {
        std::string result;
        for (size_t i = 1; i < value->size(); ++i) {
            const auto position = std::string("0123456789abcdef").find(
                static_cast<char>(std::tolower(static_cast<unsigned char>((*value)[i]))));
            if (position == std::string::npos) throw std::string("22P02");
            for (int shift = 3; shift >= 0; --shift) result += ((position >> shift) & 1) ? '1' : '0';
        }
        return result;
    }
    if (!value->empty() && (value->front() == 'b' || value->front() == 'B')) value->erase(0, 1);
    if (value->find_first_not_of("01") != std::string::npos) throw std::string("22P02");
    return value;
}
std::optional<bool> comparison(const Input& left, const Input& right, bool lower) {
    auto a = left.value, b = right.value;
    if (left.type == "bit" || right.type == "bit") {
        if (left.type == "integer" || right.type == "integer") throw std::string("42883");
        if (left.type == "unknown") a = bits(a);
        if (right.type == "unknown") b = bits(b);
    } else if (right.type == "integer" && a) {
        if (a->empty() || a->find_first_not_of("0123456789") != std::string::npos) throw std::string("22P02");
        if (!b) return {};
        return lower ? std::stoi(*a) >= std::stoi(*b) : std::stoi(*a) <= std::stoi(*b);
    }
    if (!a || !b) return {};
    return lower ? *a >= *b : *a <= *b;
}
struct Expected {std::optional<bool> value; std::string state;};
Expected expected(const Input& left, const Input& lower, const Input& upper, bool negate) {
    try {
        const auto a = comparison(left, lower, true), b = comparison(left, upper, false);
        std::optional<bool> value;
        if ((a && !*a) || (b && !*b)) value = false;
        else if (a && b) value = true;
        if (value && negate) value = !*value;
        return {value, ""};
    } catch (const std::string& state) {return {{}, state};}
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_between_input");
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;
    size_t controls = 0, failures = 0;
    const auto require = [&](bool condition, const std::string& label) {
        ++controls;
        if (!condition) {++failures; std::cerr << "[BIT BETWEEN INPUT FAIL] " << label << '\n';}
    };
    const auto cursor = [&](const std::string& sql, const std::vector<QueryBindingDatum>& datums,
                            const Expected& target) {
        std::string state; bool valid = false;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, sql, datums));
            auto rows = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
                &g_engine, database, prepared, prepared->ast.get()), prepared->output);
            std::vector<ExprValue> row;
            valid = prepared->output.size() == 1 && prepared->output[0].type == "boolean" &&
                rows->next(row) && row.size() == 1 && row[0].typeName == "boolean" &&
                row[0].isNull == !target.value && (!target.value || row[0].asBool() == *target.value) &&
                !rows->next(row);
            rows->close();
        } catch (const DbError& error) {state = error.sqlState();}
        catch (const std::exception&) {state = "UNSTRUCTURED";}
        require(state == target.state && (!state.empty() || valid), "cursor " + sql + " actual=" + state);
    };
    const std::vector<Input> lefts = {{"B'01'", "bit", "01"}, {"B'01'::varbit", "bit", "01"},
        {"NULL::bit", "bit", {}}, {"NULL::varbit", "bit", {}}, {"'01'", "unknown", "01"},
        {"NULL", "unknown", {}}, {"'b01'", "unknown", "b01"}, {"'2'", "unknown", "2"}};
    const std::vector<Input> bounds = {{"B'01'", "bit", "01"}, {"B'1'", "bit", "1"},
        {"'01'", "unknown", "01"}, {"'b01'", "unknown", "b01"}, {"'x1'", "unknown", "x1"},
        {"''", "unknown", ""}, {"'102'", "unknown", "102"}, {"NULL", "unknown", {}}, {"1", "integer", "1"}};
    for (const auto& left : lefts) for (const auto& lower : bounds) for (const auto& upper : bounds) {
        if (left.type != "bit" && lower.type != "bit" && upper.type != "bit") continue;
        for (const bool negate : {false, true}) {
            const auto target = expected(left, lower, upper, negate);
            const auto expression = left.sql + (negate ? " NOT BETWEEN " : " BETWEEN ") + lower.sql + " AND " + upper.sql;
            ExprEvalResult value;
            try {value = ExprHelper::evalString(expression, {});}
            catch (const DbError& error) {value.sqlState = error.sqlState();}
            catch (const std::exception&) {value.sqlState = "UNSTRUCTURED";}
            require(value.sqlState == target.state && value.ok == target.state.empty() &&
                (!value.ok || (value.typeName == "boolean" && value.isNull == !target.value &&
                 (!target.value || ExprValue(value.typeName, value.value).asBool() == *target.value))), "helper " + expression);
            cursor("SELECT " + expression + " AS value", {}, target);
        }
    }
    require(controls == 1808, "all 904 complete scalar cases reached both actual owners");
    for (const auto& source : std::vector<std::optional<std::string>>{"01", "b01", "2", "", {}}) {
        for (const auto& range : std::vector<std::pair<std::string, Expected>>{
            {"B'01' AND 1", {{}, "42883"}}, {"1 AND B'01'", {{}, "42883"}},
            {"NULL AND B'01'", {{}, "42883"}}, {"'b0' AND B'01'", {{}, "42883"}},
            {"B'01' AND NULL", {source && source->empty() ? std::optional<bool>(false) : std::nullopt,
                                source && *source == "2" ? "22P02" : ""}},
            {"B'01' AND 'b0'", {source ? std::optional<bool>(false) : std::nullopt,
                                   source && *source == "2" ? "22P02" : ""}}}) {
            cursor("SELECT $1 BETWEEN " + range.first + " AS value",
                {{"owned-between-parameter", "p", "unknown", {}, true, source, 1}}, range.second);
        }
        for (const auto& type : {"text", "integer"})
            cursor("SELECT $1 BETWEEN B'01' AND B'01' AS value",
                {{"owned-typed-between-parameter", "p", type, {}, true, source, 1}}, {{}, "42883"});
    }
    TableSchema sink; sink.tablename = "calls"; sink.append(makeIntColumn("id", false, 4));
    if (g_engine.createTable(database, sink) != DBStatus::OK) return 2;
    if (g_engine.createUDF(database, "between_input_writer", {"p"}, {"varbit"},
        "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;", 'v', "plpgsql", "varbit") != DBStatus::OK) return 2;
    for (const auto& range : {"'102' AND B'11'", "B'00' AND '102'", "B'01' AND 1"}) {
        std::string state;
        try {(void)g_engine.prepareBoundQuery(database, std::string("SELECT between_input_writer(B'01') BETWEEN ") + range);}
        catch (const DbError& error) {state = error.sqlState();}
        require(state == (std::string(range).find("102") == std::string::npos ? "42883" : "22P02") &&
            g_engine.query(database, "calls", {}, {"id"}, {}).empty(), "input preparation never executes writer");
    }
    for (const auto& kind : {"bit", "varbit"}) for (const auto& population : {"populated", "empty", "nulls"}) {
        TableSchema table; table.tablename = std::string(kind) + "_" + population;
        table.append(makeIntColumn("id", false, 4, true));
        Column bit; bit.dataName = "b"; bit.isNull = true;
        if (!TypeRegistry::instance().resolveColumnType(bit, kind,
            std::string(kind) == "bit" ? std::vector<std::string>{"2"} : std::vector<std::string>{}, false).empty()) return 2;
        table.append(bit);
        if (g_engine.createTable(database, table) != DBStatus::OK) return 2;
        if (std::string(population) != "empty" && g_engine.insertRow(database, table.tablename,
            {{"id", "1"}, {"b", std::string(population) == "nulls" ? std::nullopt : std::optional<std::string>("01")}}) != DBStatus::OK) return 2;
        const Input left{"b", "bit", std::string(population) == "nulls" ? std::nullopt : std::optional<std::string>("01")};
        for (const auto& indexes : std::vector<std::pair<size_t, size_t>>{{3, 4}, {5, 3}, {0, 6}, {6, 0}, {0, 8}, {8, 0}})
            for (const bool negate : {false, true}) for (const size_t cap : {size_t(0), size_t(1)}) {
                const auto target = expected(left, bounds[indexes.first], bounds[indexes.second], negate);
                StorageEngine::SelectExpr projection; projection.displayName = "value"; projection.isScalar = true;
                projection.funcName = "expreval"; projection.funcArgs = {"b" + std::string(negate ? " NOT BETWEEN " : " BETWEEN ") +
                    bounds[indexes.first].sql + " AND " + bounds[indexes.second].sql};
                StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
                std::string state; std::vector<std::vector<std::string>> rows; std::vector<std::vector<bool>> nulls;
                try {(void)g_engine.queryExpr(database, table.tablename, {}, {projection}, {}, &rows, &nulls, nullptr, options);}
                catch (const DbError& error) {state = error.sqlState();}
                catch (const std::exception&) {state = "UNSTRUCTURED";}
                const bool receives = cap && std::string(population) != "empty";
                bool valid = !receives ? rows.empty() : rows.size() == 1 && rows[0].size() == 1 &&
                    nulls.size() == 1 && nulls[0].size() == 1 && nulls[0][0] == !target.value &&
                    (!target.value || rows[0][0] == (*target.value ? "t" : "f"));
                require(state == target.state && (!state.empty() || valid), "real projection admission " + table.tablename +
                    " cap=" + std::to_string(cap) + " " + projection.funcArgs[0] + " actual=" + state);
            }
        for (const size_t cap : {size_t(0), size_t(1)}) {
            StorageEngine::SelectExpr writer; writer.displayName = "value"; writer.isScalar = true;
            writer.funcName = "expreval"; writer.funcArgs = {"between_input_writer(b) BETWEEN '102' AND B'11'"};
            StorageEngine::QueryExprExecutionOptions options; options.maxProjectionRows = cap;
            std::string state;
            try {(void)g_engine.queryExpr(database, table.tablename, {}, {writer}, {}, options);}
            catch (const DbError& error) {state = error.sqlState();}
            catch (const std::exception&) {state = "UNSTRUCTURED";}
            require(state == "22P02" && g_engine.query(database, "calls", {}, {"id"}, {}).empty(),
                "actual projection writer cannot run before input admission");
        }
    }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "[BIT BETWEEN INPUT] all " << controls << " helper/binder/cursor/pair-context/parameter/NULL/early-error controls; failures=" << failures << '\n';
    return failures ? 1 : 0;
}
