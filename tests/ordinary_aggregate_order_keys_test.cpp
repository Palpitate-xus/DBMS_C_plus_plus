#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t checked = 0;
    SQLParser parser;
    {
        auto legacy = parser.parse("SELECT sum(n ORDER BY k)");
        auto* select = dynamic_cast<SelectStmt*>(legacy.stmt.get());
        auto* call = select ? dynamic_cast<FunctionCallExpr*>(select->selectList.at(0).expr.get()) : nullptr;
        assert(legacy.success && call && call->args.size() == 2 && call->aggregateOrderKeys.empty());
        ++checked;
        auto bound = parser.parseForBinding("SELECT pg_catalog.sum(n ORDER BY k DESC NULLS LAST,n ASC)");
        select = dynamic_cast<SelectStmt*>(bound.stmt.get());
        call = select ? dynamic_cast<FunctionCallExpr*>(select->selectList.at(0).expr.get()) : nullptr;
        assert(bound.success && call && call->args.size() == 1 && call->aggregateOrderKeys.size() == 2);
        assert(call->orderBy.empty() && !call->aggregateOrderKeys[0].ascending && !call->aggregateOrderKeys[0].nullsFirst);
        assert(call->aggregateOrderKeys[1].ascending && !call->aggregateOrderKeys[1].nullsFirst);
        assert(dynamic_cast<ColumnRefExpr*>(call->aggregateOrderKeys[0].expr.get())->column == "k");
        ++checked;
        auto pending = parser.parseForBinding("SELECT sum(1 ORDER BY 1 USING =)");
        select = dynamic_cast<SelectStmt*>(pending.stmt.get());
        call = select ? dynamic_cast<FunctionCallExpr*>(select->selectList.at(0).expr.get()) : nullptr;
        assert(pending.success && call && call->aggregateOrderKeys.size() == 1 &&
               call->aggregateOrderKeys[0].usingOperator == "=");
        ++checked;
    }
    const auto database = testDbPath("ordinary_aggregate_order_keys");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    const auto previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rows_(id INTEGER,n INTEGER,k INTEGER,v DOUBLE PRECISION)", session));
    assert(!ddl.executeSql("CREATE TABLE empty_(id INTEGER,n INTEGER,k INTEGER,v DOUBLE PRECISION)", session));
    assert(g_engine.insertRow(database,"rows_",{{"id","1"},{"n","2"},{"k","2"},{"v","1"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database,"rows_",{{"id","2"},{"n","-1"},{"k","3"},{"v","-9007199254740992"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database,"rows_",{{"id","3"},{"n",std::nullopt},{"k","1"},{"v","9007199254740992"}}) == DBStatus::OK);
    const auto check = [&](const std::string& sql, const std::vector<std::vector<std::string>>& rows,
                           bool expectedNull = false) {
        auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        const auto spelling = query->legacySql();
        for (size_t repeat = 0; repeat < 2; ++repeat) {
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
                &g_engine,database,query,query->ast.get()));
            bool correct = result.structuredRowsAvailable && result.structuredRows.size() == rows.size() &&
                result.structuredNulls.size() == rows.size();
            for (size_t i = 0; correct && i < rows.size(); ++i)
                correct = result.structuredRows[i].size() == 1 && result.structuredNulls[i].size() == 1 &&
                    result.structuredNulls[i][0] == expectedNull &&
                    (expectedNull || result.structuredRows[i] == rows[i]);
            if (!result.ok || !correct)
                std::cerr << "ORDER_KEYS_SQL " << sql << " state=" << result.errorSqlState
                          << " message=" << result.errorMessage << '\n';
            result.throwIfFailed();
            assert(correct);
            assert(query->legacySql() == spelling);
            ++checked;
        }
    };
    for (const auto& function : {std::string("sum"),std::string("count")}) {
        check("SELECT "+function+"(n ORDER BY k) FROM rows_", {{function == "sum" ? "1" : "2"}});
        check("SELECT "+function+"(n ORDER BY k) FROM rows_ LIMIT 0", {});
    }
    check("SELECT count(n ORDER BY k) FROM empty_", {{"0"}});
    check("SELECT sum(DISTINCT n ORDER BY n DESC NULLS FIRST) FROM rows_", {{"1"}});
    check("SELECT sum(v ORDER BY k ASC) FROM rows_", {{"0"}});
    check("SELECT sum(v ORDER BY k DESC) FROM rows_", {{"1"}});
    check("SELECT sum(v ORDER BY 1,k ASC) FROM rows_", {{"0"}});
    check("SELECT sum(v ORDER BY 1,k DESC) FROM rows_", {{"1"}});
    check("SELECT sum(v ORDER BY CASE WHEN id=3 THEN NULL ELSE k END NULLS FIRST) FROM rows_", {{"0"}});
    check("SELECT sum(v ORDER BY CASE WHEN id=3 THEN NULL ELSE k END NULLS LAST) FROM rows_", {{"1"}});
    check("SELECT sum(v ORDER BY k USING <) FROM rows_", {{"0"}});
    check("SELECT sum(v ORDER BY k USING >) FROM rows_", {{"1"}});
    check("SELECT sum(r.v ORDER BY r.k) FROM rows_ AS r", {{"0"}});
    check("SELECT sum(v ORDER BY k) FILTER(WHERE false) FROM rows_", {{""}}, true);
    const auto error = [&](const std::string& sql,const std::string& state) {
        std::string actual;
        try { (void)g_engine.prepareBoundQuery(database,sql); }
        catch (const DbError& failure) { actual = failure.sqlState(); }
        if (actual != state) std::cerr << "ORDER_KEYS_BIND " << sql << " actual=" << actual << " expected=" << state << '\n';
        assert(actual == state);
        ++checked;
    };
    for (const auto& function : {std::string("sum"),std::string("count")})
        for (const auto& tail : {std::string("rows_"),std::string("empty_"),std::string("rows_ WHERE false"),std::string("rows_ LIMIT 0")})
            error("SELECT "+function+"(n ORDER BY missing) FROM "+tail,"42703");
    error("SELECT sum(DISTINCT n ORDER BY k) FROM rows_ LIMIT 0","42P10");
    error("SELECT sum(n ORDER BY projected) AS projected FROM rows_ LIMIT 0","42703");
    error("SELECT sum(n ORDER BY sum(k)) FROM rows_ LIMIT 0","42803");
    error("SELECT sum(n ORDER BY row_number() OVER()) FROM rows_ LIMIT 0","42803");
    error("SELECT sum(n ORDER BY unnest(ARRAY[1,2])) FROM rows_ LIMIT 0","0A000");
    for (const auto& sql : {"SELECT count(* ORDER BY 1)", "SELECT sum(ORDER BY 1)",
                           "SELECT sum(1 ORDER BY)", "SELECT sum(1 ORDER BY 1,)",
                           "SELECT sum(1 ORDER BY 1 DESC NULLS MIDDLE)"})
        error(sql,"42601");
    error("SELECT sum(1 ORDER BY 1 USING =)","42809");
    error("SELECT sum(v ORDER BY k ASC) AS a,sum(v ORDER BY k DESC) AS a FROM rows_ ORDER BY a","42702");
    check("SELECT sum(v ORDER BY k ASC)+0 FROM rows_", {{"0"}});
    check("SELECT sum(v ORDER BY k DESC)+0 FROM rows_", {{"1"}});
    check("SELECT sum(v ORDER BY k ASC)+sum(v ORDER BY k DESC) FROM rows_", {{"1"}});
    assert(checked == 61);
    std::cout << "[ORDINARY AGGREGATE ORDER KEYS] checks=" << checked << '\n';
}
