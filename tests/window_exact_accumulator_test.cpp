#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "types/numeric.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;
using Cell = std::optional<std::string>;

struct Input {
    std::string partition;
    Cell value;
    Cell sum;
    Cell average;
};

static const std::vector<Input> integers = {
    {"maximum", "9223372036854775807", "18446744073709551614", "9223372036854775807"},
    {"maximum", "9223372036854775807", "18446744073709551614", "9223372036854775807"},
    {"minimum", "-9223372036854775808", "-18446744073709551616", "-9223372036854775808"},
    {"minimum", "-9223372036854775808", "-18446744073709551616", "-9223372036854775808"},
    {"cancel", "9223372036854775807", "-1", "-0.5"},
    {"cancel", "-9223372036854775808", "-1", "-0.5"},
    {"null", std::nullopt, std::nullopt, std::nullopt},
    {"null", std::nullopt, std::nullopt, std::nullopt},
};
static const std::vector<Input> fractions = {
    {"fraction", "1.25", "4", "2"}, {"fraction", "2.75", "4", "2"},
    {"mixed", "1.25", "1.25", "1.25"},
    {"mixed", std::nullopt, "1.25", "1.25"},
};

static void check(const std::string& database, const std::string& table,
                  const std::vector<Input>& input, bool cumulative) {
    WindowFunctionSpec spec;
    spec.partitionBy = {"p"};
    spec.orderBy = "id";
    spec.hasFrame = true;
    spec.frameType = WindowFunctionSpec::FrameType::ROWS;
    spec.frameStartOffset = -1;
    spec.frameEndOffset = cumulative ? 0 : -1;
    spec.argument = "v";
    PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.orderByCol = "id";
    context.windowTargets = {{false, "id", 0}};
    for (const std::string name : {"sum", "avg"}) {
        spec.name = name;
        context.windowTargets.push_back({true, "", context.windowFunctions.size()});
        context.windowFunctions.push_back(spec);
    }
    auto plan = QueryPlanner::buildSelectPlan(&g_engine, context);
    assert(dynamic_cast<WindowOp*>(plan.get())); // exercise production WindowOp
    assert(plan->open());
    std::string rendered;
    size_t row = 0;
    while (plan->next(rendered)) {
        assert(row < input.size());
        std::vector<std::string> cells;
        std::vector<bool> nulls;
        assert(plan->lastStructuredRow(cells, nulls));
        assert(cells.size() == 3 && nulls.size() == 3);
        assert(!nulls[0] && cells[0] == std::to_string(row + 1));
        const bool first = cumulative && row % 2 == 0;
        const std::vector<Cell> wanted = {
            first ? input[row].value : input[row].sum,
            first ? input[row].value : input[row].average};
        for (size_t column = 0; column < wanted.size(); ++column) {
            assert(nulls[column + 1] == !wanted[column]);
            if (wanted[column]) {
                if (!(Numeric(cells[column + 1]) == Numeric(*wanted[column]))) {
                    std::cerr << table << " cumulative=" << cumulative << " row=" << row
                              << " actual=" << cells[column + 1]
                              << " expected=" << *wanted[column] << '\n';
                    assert(false);
                }
            }
        }
        ++row;
    }
    assert(!plan->hasError() && row == input.size());
    plan->close();
}

int main() {
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("window_exact_accumulator");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE ints(id INT,p TEXT,v BIGINT)", session));
    assert(!ddl.executeSql("CREATE TABLE fractions(id INT,p TEXT,v NUMERIC)", session));
    assert(!ddl.executeSql("CREATE TABLE empty_t(id INT,p TEXT,v BIGINT)", session));
    for (const auto& fixture : std::vector<std::pair<std::string, std::vector<Input>>>{
            {"ints", integers}, {"fractions", fractions}}) {
        for (size_t row = 0; row < fixture.second.size(); ++row) {
            const auto& value = fixture.second[row];
            assert(g_engine.insertRow(database, fixture.first,
                {{"id", std::to_string(row + 1)}, {"p", value.partition}, {"v", value.value}})
                == DBStatus::OK);
        }
        for (const bool cumulative : {false, true})
            check(database, fixture.first, fixture.second, cumulative);
    }
    for (const bool cumulative : {false, true}) check(database, "empty_t", {}, cumulative);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb("window_exact_accumulator");
    std::cout << "[WINDOW EXACT ACCUMULATOR] 6 real planner controls / 24 rows passed\n";
}
