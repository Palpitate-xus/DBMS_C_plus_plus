#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_predicate_pre_scan_type");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema table;
    table.tablename = "bits";
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeTextColumn("t", true));
    table.append(makeIntColumn("i", true, 4));
    Column bit;
    bit.dataName = "v"; bit.isNull = true;
    assert(TypeRegistry::instance().resolveColumnType(bit, "varbit", {}, false).empty());
    table.append(bit);
    for (const auto* name : {"bits", "empty_bits"}) {
        table.tablename = name;
        assert(g_engine.createTable(database, table) == DBStatus::OK);
        assert(g_engine.createIndex(database, name, "t") == DBStatus::OK);
    }
    assert(g_engine.insertRow(database, "bits", {{"id", "1"}, {"t", "01"}, {"i", "1"}, {"v", "01"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "bits", {{"id", "2"}, {"t", std::nullopt}, {"i", std::nullopt}, {"v", std::nullopt}}) == DBStatus::OK);
    size_t controls = 0;
    const auto mustReject = [&](const auto& action) {
        ++controls;
        bool rejected = false;
        try { action(); }
        catch (const DbError& error) { rejected = true; assert(error.sqlState() == "42883"); }
        assert(rejected);
    };
    for (const auto* name : {"bits", "empty_bits"}) {
        PlanContext context;
        context.dbname = database; context.tablename = name;
        context.selectCols = {"id"}; context.orderByCol = "id";
        for (const auto* column : {"id", "t", "i"}) {
            for (const auto* literal : {"B'01'", "X'1'", "B''", "X''"}) {
                for (const auto* operation : {"=", "<>", "<", "<=", ">", ">=", "in", "notin", "between", "notbetween"}) {
                    const std::string operands = std::string(operation).find("between") == std::string::npos
                        ? literal : std::string(literal) + " B'1'";
                    context.conds = StorageEngine::parseConditions({std::string(operation) + column + " " + operands});
                    assert(context.conds.size() == 1 && context.conds[0].patternType == "bit");
                    mustReject([&] { (void)QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context)); });
                }
            }
            for (const auto* operation : {"BETWEEN", "NOT BETWEEN"}) {
                const std::string expression = std::string(column) + " " + operation + " B'' AND B'01'";
                context.conds = StorageEngine::parseConditions({"typedexpr " + expression});
                mustReject([&] { (void)QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context)); });
                mustReject([&] { (void)g_engine.prepareBoundQuery(database, "SELECT id FROM " + std::string(name) + " WHERE " + expression); });
            }
        }
        auto invalid = StorageEngine::parseConditions({"=t B'01'"});
        context.conds = StorageEngine::parseConditions({"=id 1", "=t B'01'"});
        mustReject([&] { (void)QueryPlanner::buildSelectPlan(&g_engine, context); });
        context.conds.clear();
        context.disjunctiveConds = {StorageEngine::parseConditions({"=id 1"}), invalid};
        mustReject([&] { (void)QueryPlanner::buildSelectPlan(&g_engine, context); });
        mustReject([&] { (void)QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, context, context.disjunctiveConds); });
        context.disjunctiveConds.clear();
        context.conds = StorageEngine::parseConditions({"=id 1"});
        auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context));
        assert(result.ok && result.rows.size() == (std::string(name) == "bits" ? 1 : 0));
        ++controls;
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[BIT PREDICATE PRE-SCAN TYPE] all " << controls
              << " index/scan/list/BETWEEN/empty/NULL/prepared/disjunctive controls passed\n";
}
