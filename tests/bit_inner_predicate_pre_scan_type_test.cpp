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
    const auto database = testDbPath("bit_inner_predicate_pre_scan_type");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema table;
    table.append(makeIntColumn("id", false, 4, true));
    Column bit;
    bit.dataName = "v"; bit.isNull = true;
    assert(TypeRegistry::instance().resolveColumnType(bit, "varbit", {}, false).empty());
    table.append(bit);
    for (const auto* name : {"bits", "empty_bits"}) {
        table.tablename = name;
        assert(g_engine.createTable(database, table) == DBStatus::OK);
    }
    assert(g_engine.insertRow(database, "bits", {{"id", "1"}, {"v", "01"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "bits", {{"id", "2"}, {"v", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.createUDF(database, "inner_identity_bit", {"p"}, {"varbit"},
        "BEGIN RETURN p; END;", 'v', "plpgsql", "varbit") == DBStatus::OK);
    size_t controls = 0;
    for (const auto* inner : {"bits", "empty_bits"}) {
        for (int path = 0; path != 4; ++path) {
            const auto contextFor = [&](bool valid) {
                PlanContext context;
                context.dbname = database; context.tablename = "bits"; context.selectCols = {"id"};
                const auto conditions = StorageEngine::parseConditions({valid
                    ? "typedexpr inner_identity_bit(v)=B'01'" : "typedexpr id BETWEEN B'' AND B'01'"});
                if (path == 0) context.semiJoins.push_back({database, inner, "v", "v", conditions});
                if (path == 1) {
                    ExistenceSpec spec; spec.dbname = database; spec.tablename = inner; spec.innerConds = conditions;
                    context.existenceFilters.push_back(spec);
                }
                if (path == 2) context.quantifiedSubqueries.push_back({database, inner, "v", "v", "=", conditions});
                if (path == 3) {
                    context.projectionTargets = {{false, "id"}, {true, ""}};
                    context.scalarSubquery = {database, inner, "v", conditions};
                }
                return context;
            };
            ++controls;
            bool rejected = false;
            try { (void)QueryPlanner::buildSelectPlan(&g_engine, contextFor(false)); }
            catch (const DbError& error) { rejected = true; assert(error.sqlState() == "42883"); }
            assert(rejected);
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, contextFor(true)));
            result.throwIfFailed(); assert(result.ok);
            const bool populated = std::string(inner) == "bits";
            if (path < 3) assert(result.rows == (!populated ? std::vector<std::string>{} : path == 1
                ? std::vector<std::string>{"1 ", "2 "} : std::vector<std::string>{"1 "}));
            else {
                assert(result.rows == (populated ? std::vector<std::string>{"1 01 ", "2 01 "}
                                               : std::vector<std::string>{"1 NULL ", "2 NULL "}));
                assert(result.structuredRowsAvailable);
                assert(result.structuredNulls == std::vector<std::vector<bool>>({{false, !populated}, {false, !populated}}));
            }
            ++controls;
        }
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[BIT INNER PREDICATE PRE-SCAN TYPE] all " << controls
              << " real semi/existence/quantified/scalar child owner/empty/NULL/type controls passed\n";
}
