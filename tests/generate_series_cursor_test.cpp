#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const auto database=testDbPath("generate_series_cursor");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    const auto plan=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        auto result=QueryPlanner::buildPreparedSetReturningPlan(&g_engine,database,query,select);
        assert(result);return result;
    };
    for(const auto* sql:{"SELECT generate_series(1,2,0) LIMIT 0",
        "SELECT generate_series(1,2,0) WHERE false",
        "SELECT generate_series(1,2,0) WHERE NULL::BOOL"}) {
        auto actual=plan(sql);assert(actual->open());std::string row;
        assert(!actual->next(row));actual->close();
    }
    // Opening and fetching one row from an enormous range must stay bounded.
    auto actual=plan("SELECT generate_series(1,9223372036854775807) LIMIT 1");
    for(int reopen=0;reopen<2;++reopen) {
        assert(actual->open());std::string row;assert(actual->next(row));
        std::vector<ExprValue> values;
        assert(actual->lastStructuredValues(values) && values.front().value=="1");
        assert(!actual->next(row));actual->close();
    }
    auto result=QueryPlanner::executePlanChecked(plan("SELECT generate_series(1,2),generate_series(10,12)"));
    result.throwIfFailed();assert(result.structuredRowsAvailable);
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"1","10"},{"2","11"},{"","12"}}));
    assert(result.structuredNulls.back().front());
    result=QueryPlanner::executePlanChecked(plan("SELECT generate_series(2147483646,2147483647)"));
    result.throwIfFailed();assert(result.structuredRows.size()==2);
    assert(result.structuredRows.back().front()=="2147483647");
    result=QueryPlanner::executePlanChecked(plan("SELECT generate_series(1.0::NUMERIC,2.0::NUMERIC,0.25::NUMERIC)"));
    result.throwIfFailed();
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"1.0"},{"1.25"},{"1.50"},{"1.75"},{"2.00"}}));
    result=QueryPlanner::executePlanChecked(plan("SELECT generate_series(NULL::INT,1,0)"));
    result.throwIfFailed();assert(result.structuredRows.empty());
    result=QueryPlanner::executePlanChecked(plan("SELECT generate_series(1,2,0)"));
    assert(!result.ok);
    try {result.throwIfFailed();assert(false);} catch(const DbError& error) {
        assert(error.sqlState()=="22023");
    }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[GENERATE SERIES CURSOR] lazy demand, reopen, zip NULL, exact numeric and bounded overflow passed\n";
}
