#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include <cassert>
#include <iostream>
#include <set>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    // Construct the real retained child graph. Its reached constant CASE
    // raises 22012 while the graph is prepared, before a source is opened.
    // A child removed from the parent's compiled root must not be acquired.
    for (const bool planRoot : {false, true}) {
        for (const auto& sql : std::vector<std::string>{
            "SELECT CASE WHEN false THEN 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END) ELSE true END",
            "SELECT CASE WHEN true THEN true ELSE 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END) END",
        }) {
            auto query = std::make_shared<PreparedQuery>(prepareQuery(sql, {}, {}));
            auto* select = static_cast<SelectStmt*>(query->ast.get());
            auto* expression = select->selectList.front().expr.get();
            PreparedQueryExecution execution(query, &owner, "unused");
            size_t creates = 0;
            execution.setChildCursorFactory([&](const Stmt* child, const RowContext& outer) {
                ++creates;
                auto plan = QueryPlanner::buildPreparedQueryPlan(&owner, "unused", query, child, outer);
                return QueryPlanner::makePreparedCursor(std::move(plan), query->statementOutputs.at(child));
            });
            if (planRoot) execution.planStatementConstants(select);
            execution.prepareExpression(expression);
            std::string state;
            try { execution.prepareChildCursors(); }
            catch (const DbError& error) { state = error.sqlState(); }
            std::cout << sql << " plan=" << planRoot << " creates=" << creates
                      << " state=" << state << std::endl;
            assert(state.empty() && creates == 0);
            const auto value = execution.evaluate(expression, execution.context());
            assert(value.typeName == "boolean" && !value.isNull && value.asBool());
            execution.closeChildCursors();
        }
    }
    {
        auto query = std::make_shared<PreparedQuery>(prepareQuery(
            "SELECT 1 WHERE false AND 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)", {}, {}));
        auto* select = static_cast<SelectStmt*>(query->ast.get());
        PreparedQueryExecution execution(query, &owner, "unused");
        size_t creates = 0;
        execution.setChildCursorFactory([&](const Stmt*, const RowContext&) -> std::unique_ptr<PreparedQueryCursor> {
            ++creates;
            throw DbError("22012", "discarded child graph was acquired");
        });
        execution.planStatementConstants(select);
        execution.prepareExpression(select->whereClause.get());
        execution.prepareChildCursors();
        assert(creates == 0 && !execution.evaluate(select->whereClause.get(), execution.context()).asBool());
    }
    // Reached original sites retain distinct cursors and values. A repeated
    // prepare call acquires no second graph, and changing the factory clears
    // the old execution's memo/cursors instead of retaining stale values.
    {
        auto query = std::make_shared<PreparedQuery>(prepareQuery(
            "SELECT 1=ANY(SELECT 1),1=ANY(SELECT 1)", {}, {}));
        auto* select = static_cast<SelectStmt*>(query->ast.get());
        PreparedQueryExecution execution(query, &owner, "unused");
        size_t creates = 0;
        std::set<const Stmt*> sites;
        const auto factory = [&](const Stmt* child, const RowContext& outer) {
            ++creates;
            sites.insert(child);
            return QueryPlanner::makePreparedCursor(
                QueryPlanner::buildPreparedQueryPlan(&owner, "unused", query, child, outer),
                query->statementOutputs.at(child));
        };
        execution.setChildCursorFactory(factory);
        execution.planStatementConstants(select);
        for (auto& item : select->selectList) execution.prepareExpression(item.expr.get());
        execution.prepareChildCursors();
        execution.prepareChildCursors();
        assert(creates == 2 && sites.size() == 2);
        for (const auto& item : select->selectList)
            assert(execution.evaluate(item.expr.get(), execution.context()).asBool());
        execution.setChildCursorFactory(factory);
        execution.prepareChildCursors();
        assert(creates == 4 && sites.size() == 2);
        execution.closeChildCursors();
    }
    // Whole-query analysis is earlier than reachability: invalid identifiers
    // and unknown input casts in a dead branch must still be rejected.
    for (const auto& control : std::vector<std::pair<std::string, std::string>>{
        {"SELECT CASE WHEN false THEN 1=ANY(SELECT missing_column) ELSE true END", "42703"},
        {"SELECT CASE WHEN false THEN 1=ANY(SELECT CAST('bad' AS INT)) ELSE true END", "22P02"},
    }) {
        std::string state;
        try { (void)owner.prepareBoundQuery("unused", control.first); }
        catch (const DbError& error) { state = error.sqlState(); }
        assert(state == control.second);
    }
    assert(!owner.inTransaction());
    std::cout << "[PREPARED QUANTIFIED CURSOR DEMAND] passed\n";
}
