#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;

struct Input {
    int id;
    std::optional<std::string> partition;
    std::optional<int> key;
    std::optional<int> value;
    int ascendingGroup;
    int groupCount;
};

static const std::vector<Input> input = {
    {1,"a",1,10,0,4}, {2,"a",1,std::nullopt,0,4},
    {3,"a",2,20,1,4}, {4,"a",3,30,2,4}, {5,"a",3,40,2,4},
    {6,"b",1,100,0,2}, {7,"b",2,200,1,2},
    {8,"b",2,std::nullopt,1,2}, {9,std::nullopt,1,7,0,2},
    {10,std::nullopt,2,std::nullopt,1,2},
    {11,"a",std::nullopt,90,3,4},
    {12,"a",std::nullopt,std::nullopt,3,4}
};

static std::vector<std::optional<std::string>> expected(
        const Input& current, int start, int end, bool ascending,
        const std::string& exclusion) {
    const auto group = [ascending](const Input& row) {
        return ascending ? row.ascendingGroup
                         : row.groupCount - row.ascendingGroup - 1;
    };
    int count = 0, nonnull = 0, sum = 0;
    std::optional<int> minimum, maximum;
    for (const auto& row : input) {
        if (row.partition != current.partition) continue;
        if (start >= 0 && group(row) < group(current) - start) continue;
        if (end >= 0 && group(row) > group(current) + end) continue;
        if (exclusion == "current row" && row.id == current.id) continue;
        if (exclusion == "group" && row.key == current.key) continue;
        if (exclusion == "ties" && row.key == current.key && row.id != current.id) continue;
        ++count;
        if (!row.value) continue;
        ++nonnull;
        sum += *row.value;
        if (!minimum || *row.value < *minimum) minimum = row.value;
        if (!maximum || *row.value > *maximum) maximum = row.value;
    }
    const auto text = [](std::optional<int> value) -> std::optional<std::string> {
        if (!value) return std::nullopt;
        return std::to_string(*value);
    };
    return {std::to_string(current.id), text(nonnull ? std::optional<int>(sum) : std::nullopt),
            std::to_string(count), std::to_string(nonnull), text(minimum), text(maximum)};
}

int main() {
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("window_groups_unbounded");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT,p TEXT,k INT,v INT)",session));
    assert(!ddl.executeSql("CREATE TABLE empty_t(id INT,p TEXT,k INT,v INT)",session));
    for (const auto& row : input) {
        const auto integer = [](std::optional<int> value) -> std::optional<std::string> {
            if (!value) return std::nullopt;
            return std::to_string(*value);
        };
        assert(g_engine.insertRow(database,"t",{{"id",std::to_string(row.id)},
            {"p",row.partition},{"k",integer(row.key)},{"v",integer(row.value)}}) == DBStatus::OK);
    }
    size_t controls = 0;
    for (bool ascending : {true,false}) for (int start : {-1,0,1})
    for (int end : {-1,0,1}) for (const std::string exclusion :
            {"no others","current row","group","ties"}) {
        WindowFunctionSpec spec;
        spec.partitionBy = {"p"};
        spec.orderBy = "k";
        spec.orderAscending = ascending;
        spec.hasFrame = true;
        spec.frameType = WindowFunctionSpec::FrameType::GROUPS;
        spec.frameStartOffset = start;
        spec.frameEndOffset = end;
        spec.frameExclusion = exclusion;
        PlanContext context;
        context.dbname = database;
        context.tablename = "t";
        context.orderByCol = "id";
        context.windowTargets = {{false,"id",0}};
        for (const auto& aggregate : std::vector<std::pair<std::string,std::string>>{
                {"sum","v"},{"count","*"},{"count","v"},{"min","v"},{"max","v"}}) {
            spec.name = aggregate.first;
            spec.argument = aggregate.second;
            context.windowTargets.push_back({true,"",context.windowFunctions.size()});
            context.windowFunctions.push_back(spec);
        }
        auto plan = QueryPlanner::buildSelectPlan(&g_engine,context);
        assert(dynamic_cast<WindowOp*>(plan.get()));
        assert(plan->open());
        std::string rendered;
        size_t row = 0;
        while (plan->next(rendered)) {
            assert(row < input.size());
            std::vector<std::string> cells;
            std::vector<bool> nulls;
            assert(plan->lastStructuredRow(cells,nulls));
            const auto wanted = expected(input[row],start,end,ascending,exclusion);
            assert(cells.size() == wanted.size() && nulls.size() == wanted.size());
            for (size_t cell = 0; cell < wanted.size(); ++cell) {
                if (nulls[cell] != !wanted[cell] ||
                        (wanted[cell] && cells[cell] != *wanted[cell])) {
                    std::cerr << "GROUPS_UNBOUNDED start=" << start << " end=" << end
                        << " asc=" << ascending << " exclusion=" << exclusion
                        << " id=" << input[row].id << " column=" << cell
                        << " actual=" << (nulls[cell] ? "<SQL NULL>" : cells[cell])
                        << " expected=" << (wanted[cell] ? *wanted[cell] : "<SQL NULL>") << '\n';
                    assert(false);
                }
            }
            ++row;
        }
        assert(!plan->hasError() && row == input.size());
        plan->close();
        context.tablename = "empty_t";
        auto empty = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));
        assert(empty.ok && empty.rows.empty());
        ++controls;
    }
    assert(controls == 72);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb("window_groups_unbounded");
    std::cout << "[WINDOW GROUPS UNBOUNDED] complete72 real planner/NULL/frame/empty controls passed\n";
}
