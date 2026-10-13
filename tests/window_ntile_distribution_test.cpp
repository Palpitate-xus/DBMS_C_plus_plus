#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
using namespace dbms;

struct Case {
    int buckets;
    std::vector<int> six;
    std::vector<int> seven;
};

int main() {
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("window_ntile_distribution");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT,p TEXT)", session));
    assert(!ddl.executeSql("CREATE TABLE empty_t(id INT,p TEXT)", session));
    for (int id = 1; id <= 14; ++id) {
        assert(g_engine.insertRow(database, "t", {{"id", std::to_string(id)},
            {"p", id <= 6 ? "a" : id <= 13 ? "b" : "c"}}) == DBStatus::OK);
    }
    const std::vector<Case> cases = {
        {1, {1,1,1,1,1,1}, {1,1,1,1,1,1,1}},
        {2, {1,1,1,2,2,2}, {1,1,1,1,2,2,2}},
        {3, {1,1,2,2,3,3}, {1,1,1,2,2,3,3}},
        {4, {1,1,2,2,3,4}, {1,1,2,2,3,3,4}},
        {8, {1,2,3,4,5,6}, {1,2,3,4,5,6,7}},
        {2147483647, {1,2,3,4,5,6}, {1,2,3,4,5,6,7}},
    };
    size_t controls = 0;
    for (const auto& test : cases) for (bool ascending : {true, false}) {
        WindowFunctionSpec spec;
        spec.name = "ntile";
        spec.argument = std::to_string(test.buckets);
        spec.partitionBy = {"p"};
        spec.orderBy = "id";
        spec.orderAscending = ascending;
        PlanContext context;
        context.dbname = database;
        context.tablename = "t";
        context.orderByCol = "id";
        context.windowTargets = {{false,"id",0}, {true,"",0}};
        context.windowFunctions = {spec};
        auto plan = QueryPlanner::buildSelectPlan(&g_engine, context);
        assert(dynamic_cast<WindowOp*>(plan.get()));
        assert(plan->open());
        std::string rendered;
        int id = 1;
        while (plan->next(rendered)) {
            assert(id <= 14);
            std::vector<std::string> values;
            std::vector<bool> nulls;
            assert(plan->lastStructuredRow(values, nulls));
            assert(values.size() == 2 && nulls == std::vector<bool>({false, false}));
            int wanted = 1;
            if (id <= 6) wanted = test.six[ascending ? id - 1 : 6 - id];
            else if (id <= 13) wanted = test.seven[ascending ? id - 7 : 13 - id];
            if (values[0] != std::to_string(id) || values[1] != std::to_string(wanted)) {
                std::cerr << "NTILE buckets=" << test.buckets << " asc=" << ascending
                          << " id=" << id << " got=" << values[1]
                          << " wanted=" << wanted << '\n';
                assert(false);
            }
            ++id;
        }
        assert(!plan->hasError() && id == 15);
        plan->close();
        ++controls;
        context.tablename = "empty_t";
        auto empty = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context));
        assert(empty.ok && empty.rows.empty());
        ++controls;
    }
    assert(controls == 24);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb("window_ntile_distribution");
    std::cout << "[WINDOW NTILE DISTRIBUTION] 24 real planner controls passed\n";
}
