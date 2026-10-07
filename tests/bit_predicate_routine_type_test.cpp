#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_predicate_routine_type");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema table;
    table.append(makeIntColumn("id", false, 4, true));
    Column column;
    column.dataName = "v"; column.isNull = true;
    assert(TypeRegistry::instance().resolveColumnType(column, "varbit", {}, false).empty());
    table.append(column);
    for (const auto* name : {"bits", "empty_bits"}) {
        table.tablename = name;
        assert(g_engine.createTable(database, table) == DBStatus::OK);
    }
    assert(g_engine.insertRow(database, "bits", {{"id", "1"}, {"v", "01"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "bits", {{"id", "2"}, {"v", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.createUDF(database, "predicate_identity_bit", {"p"}, {"varbit"},
        "BEGIN RETURN p; END;", 'v', "plpgsql", "varbit") == DBStatus::OK);
    size_t controls = 0;
    for (const auto* name : {"bits", "empty_bits"}) {
        for (const auto* suffix : {"=B'01'", " BETWEEN B'01' AND B'01'",
                                  " IN (B'',B'01')", " NOT IN (B'',NULL)"}) {
            PlanContext context;
            context.dbname = database; context.tablename = name;
            context.selectCols = {"id"};
            context.conds = StorageEngine::parseConditions({std::string("typedexpr predicate_identity_bit(v)") + suffix});
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context));
            result.throwIfFailed();
            assert(result.ok);
            const bool matches = std::string(name) == "bits" && std::string(suffix).find("NOT IN") == std::string::npos;
            assert(result.rows == (matches ? std::vector<std::string>{"1 "} : std::vector<std::string>{}));
            ++controls;
        }
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[BIT PREDICATE ROUTINE TYPE] all " << controls << " real-routine planner value/NULL/empty controls passed\n";
}
