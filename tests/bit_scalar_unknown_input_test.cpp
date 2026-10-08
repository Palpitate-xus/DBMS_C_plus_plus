#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_scalar_unknown_input");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    const auto integerColumn = [](bool primaryKey) {
        Column column; column.dataName = "id"; column.isNull = false; column.isPrimaryKey = primaryKey;
        assert(TypeRegistry::instance().resolveColumnType(column, "integer", {}, false).empty());
        assert(column.dataType == "integer" && column.dsize == 4 && !column.isVariableLength);
        return column;
    };
    TableSchema schema; schema.tablename = "bits";
    schema.append(integerColumn(true));
    for (const auto& name : {"v", "u"}) {
        Column column; column.dataName = name; column.isNull = true;
        assert(TypeRegistry::instance().resolveColumnType(column, "varbit", {}, false).empty());
        schema.append(column);
    }
    schema.append(makeTextColumn("p", true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    auto empty = schema; empty.tablename = "empty_bits";
    assert(g_engine.createTable(database, empty) == DBStatus::OK);
    const std::vector<std::optional<std::string>> values = {"01", "001", "", std::nullopt, "0001", "1", "0", "00"};
    for (size_t i = 0; i < values.size(); ++i)
        assert(g_engine.insertRow(database, "bits", {{"id", std::to_string(i + 1)}, {"v", values[i]},
               {"u", values[i]}, {"p", std::string("b01")}}) == DBStatus::OK);
    assert(g_engine.createIndex(database, "bits", "v") == DBStatus::OK);
    size_t controls = 0, failures = 0;
    const auto require = [&](bool valid, const std::string& label) {
        ++controls; if (!valid) { ++failures; std::cerr << "BIT_SCALAR_INPUT_FAILURE " << label << '\n'; }
    };
    const auto truth = [](const std::string& op, const std::string& lhs, const std::string& rhs) {
        const int order = (lhs > rhs) - (lhs < rhs);
        return op == "=" ? order == 0 : op == "<>" || op == "!=" ? order != 0 :
               op == "<" ? order < 0 : op == ">" ? order > 0 : op == "<=" ? order <= 0 : order >= 0;
    };
    for (const auto& input : std::vector<std::pair<std::string, std::string>>{
            {"b01", "01"}, {"B001", "001"}, {"b", ""}, {"B", ""},
            {"x1", "0001"}, {"x", ""}, {"X", ""}, {"X0aF", "000010101111"}, {"01", "01"}, {"", ""}})
        for (const auto& column : {"v", "u"})
            for (const auto& op : {"=", "<>", "!=", "<", ">", "<=", ">="}) {
                const std::string condition = std::string(op) + column + " '" + input.first + "'";
                auto parsed = StorageEngine::parseConditions({condition});
                require(parsed.size() == 1 && parsed[0].patternType == "unknown" && parsed[0].decodedLiteralRhs,
                        "real SQL UNKNOWN literal ownership " + condition);
                std::vector<std::string> expected;
                for (size_t i = 0; i < values.size(); ++i)
                    if (values[i] && truth(op, *values[i], input.second)) expected.push_back(std::to_string(i + 1));
                auto actual = g_engine.query(database, "bits", {condition}, {"id"}, {}, false, false, false,
                    0, {}, nullptr, nullptr, nullptr);
                for (auto& row : actual) while (!row.empty() && row.back() == ' ') row.pop_back();
                std::sort(actual.begin(), actual.end());
                require(actual == expected, condition + " exact rows and NULL exclusions");
                PlanContext context; context.dbname = database; context.tablename = "bits";
                context.selectCols = {"id"}; context.conds = parsed;
                auto plan = QueryPlanner::buildSelectPlan(&g_engine, context);
                const auto* projection = dynamic_cast<const ProjectOp*>(plan.get());
                if (std::string(op) == "=" && std::string(column) == "v" && !input.second.empty())
                    require(projection && dynamic_cast<const IndexScanOp*>(projection->child()),
                            condition + " genuine secondary equality access");
                auto result = QueryPlanner::executePlanChecked(std::move(plan));
                for (auto& row : result.rows) while (!row.empty() && row.back() == ' ') row.pop_back();
                std::sort(result.rows.begin(), result.rows.end());
                require(result.ok && result.rows == expected, condition + " public planner/index/residual consumer");
                require(parsed[0].value == input.first && parsed[0].patternType == "unknown",
                        condition + " caller-owned source input is unchanged");
            }
    for (const auto& table : {"bits", "empty_bits"})
        for (const auto& invalid : {"xg", "b02", "x 1", "b 01", "B''01''", "NULL"}) {
            std::string state;
            try { (void)g_engine.query(database, table, {std::string("=v '") + invalid + "'"}, {"id"}, {},
                                      false, false, false, 0, {}, nullptr, nullptr, nullptr); }
            catch (const DbError& error) { state = error.sqlState(); }
            require(state == "22P02", std::string(table) + " invalid input admitted before rows: " + invalid);
            PlanContext context; context.dbname = database; context.tablename = table; context.selectCols = {"id"};
            context.conds = StorageEngine::parseConditions({std::string("=v '") + invalid + "'"});
            state.clear();
            try { (void)QueryPlanner::buildSelectPlan(&g_engine, context); }
            catch (const DbError& error) { state = error.sqlState(); }
            require(state == "22P02", std::string(table) + " public plan preparation before rows: " + invalid);
            context.disjunctiveConds = {StorageEngine::parseConditions({"=id 99"}), context.conds}; context.conds.clear();
            state.clear();
            try { (void)QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, context, context.disjunctiveConds); }
            catch (const DbError& error) { state = error.sqlState(); }
            require(state == "22P02", std::string(table) + " disjunctive preparation before path choice: " + invalid);
        }
    auto native = StorageEngine::parseConditions({"apicond =v b01"});
    require(native.size() == 1 && native[0].patternType.empty() && native[0].value == "b01", "native data is not SQL UNKNOWN");
    require(g_engine.query(database, "bits", {"apicond =v b01"}, {"id"}).empty(), "native literal-looking data remains data");
    auto text = g_engine.query(database, "bits", {"=p 'b01'"}, {"id"}, {}, false, false, false,
                              0, {}, nullptr, nullptr, nullptr);
    require(text.size() == values.size(), "actual TEXT column is not decoded as BIT");
    TableSchema primary; primary.tablename = "bit_primary";
    Column key; key.dataName = "k"; key.isNull = false; key.isPrimaryKey = true;
    assert(TypeRegistry::instance().resolveColumnType(key, "bit", {"4"}, false).empty());
    primary.append(key); primary.append(integerColumn(false));
    assert(g_engine.createTable(database, primary) == DBStatus::OK);
    assert(g_engine.insertRow(database, "bit_primary", {{"k", "0001"}, {"id", "1"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "bit_primary", {{"k", "0100"}, {"id", "2"}}) == DBStatus::OK);
    for (const auto& input : std::vector<std::pair<std::string, std::vector<std::string>>>{
            {"x1", {"1"}}, {"b0100", {"2"}}, {"b01", {}}, {"b", {}}}) {
        PlanContext context; context.dbname = database; context.tablename = "bit_primary"; context.selectCols = {"id"};
        context.conds = StorageEngine::parseConditions({"=k '" + input.first + "'"});
        auto plan = QueryPlanner::buildSelectPlan(&g_engine, context);
        const auto* projection = dynamic_cast<const ProjectOp*>(plan.get());
        if (input.first != "b") require(projection && dynamic_cast<const IndexScanOp*>(projection->child()), "genuine BIT primary access " + input.first);
        auto result = QueryPlanner::executePlanChecked(std::move(plan));
        for (auto& row : result.rows) while (!row.empty() && row.back() == ' ') row.pop_back();
        require(result.ok && result.rows == input.second, "primary BIT(4) comparison uses unconstrained UNKNOWN input " + input.first);
    }
    PlanContext disjunction; disjunction.dbname = database; disjunction.tablename = "bits"; disjunction.selectCols = {"id"};
    disjunction.orderByCol = "id";
    const auto branches = std::vector<std::vector<StorageEngine::Condition>>{
        StorageEngine::parseConditions({"=v 'b01'"}), StorageEngine::parseConditions({"=v 'x1'"})};
    auto orPlan = QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, disjunction, branches);
    require(bool(orPlan), "actual indexed OR plan exists");
    if (orPlan) {
        const auto result = QueryPlanner::executePlanChecked(std::move(orPlan));
        require(result.ok && result.rows == std::vector<std::string>{"1 ", "5 "}, "actual indexed OR exact canonical keys");
    }
    std::cout << "[BIT SCALAR UNKNOWN INPUT] complete controls=" << controls << " failures=" << failures << '\n';
    return failures ? 1 : 0;
}
