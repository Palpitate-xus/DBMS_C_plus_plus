#include "executor/ExecutionPlan.h"
#include "parser/query_binding.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); StorageEngine engine;
    const auto buildRoot = [&](PreparedQuery query) {
        auto* select = dynamic_cast<SelectStmt*>(query.ast.get());
        assert(select && !select->fromClause);
        auto source = std::make_unique<PreparedSourceRowsOp>(QueryRowDescriptor{},
            [](size_t index,std::vector<ExprValue>& row) {row.clear();return index==0;});
        return QueryPlanner::buildPreparedSelectPlan(&engine,"",
            std::make_shared<PreparedQuery>(std::move(query)),select,TableSchema{},
            std::move(source),{},{},{},true,true);
    };
    for (const auto& sql : {
        "SELECT 1/0 WHERE false", "SELECT 1/0 LIMIT 0",
        "SELECT (SELECT 1/0) WHERE false",
        "SELECT CASE WHEN random()>0 THEN 1/0 ELSE 1 END WHERE false"}) {
        auto query = engine.prepareBoundQuery("",sql);
        bool rejected = false;
        try { (void)buildRoot(std::move(query)); }
        catch (const DbError& error) { rejected = error.sqlState() == "22012"; }
        assert(rejected);
    }
    for (const auto& sql : {
        "SELECT CASE WHEN false THEN 1/0 ELSE 1 END WHERE false",
        "SELECT CASE WHEN true THEN 1 ELSE (SELECT 1/0) END LIMIT 0",
        "SELECT coalesce(1,1/0) WHERE false",
        "SELECT (SELECT 1) WHERE false"}) {
        auto query = engine.prepareBoundQuery("",sql);
        auto result = QueryPlanner::executePlanChecked(buildRoot(std::move(query)));
        result.throwIfFailed();
        assert(result.structuredRowsAvailable && result.rows.empty() && result.structuredRows.empty());
    }
    std::cout << "execution-root constant demand keeps errors and discarded child boundaries\n";
}
