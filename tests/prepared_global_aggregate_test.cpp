#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("prepared_global_aggregate");
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
    const auto previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE a(id INT,v BIGINT,b BOOL,t TEXT)", session));
    assert(g_engine.insertRow(db,"a",{{"id","1"},{"v","9007199254740993"},{"b","t"},{"t","NULL"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db,"a",{{"id","2"},{"v","2"},{"b","f"},{"t",""}}) == DBStatus::OK);
    assert(g_engine.insertRow(db,"a",{{"id","3"},{"v",std::nullopt},{"b",std::nullopt},{"t",std::nullopt}}) == DBStatus::OK);
    size_t checked = 0;
    const auto check = [&](const std::string& sql, const std::vector<std::vector<std::string>>& rows,
                           const std::vector<std::string>& types) {
        auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db, sql));
        std::cout << "GLOBAL_AGGREGATE_NATIVE_SQL " << sql << std::endl;
        const auto original = prepared->legacySql();
        assert(prepared->output.size() == types.size());
        for (size_t i = 0; i < types.size(); ++i) assert(prepared->output[i].type == types[i]);
        for (int run = 0; run < 2; ++run) {
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine, db, prepared, prepared->ast.get()));
            if (!result.ok) std::cout << "GLOBAL_AGGREGATE_NATIVE_ERROR " << sql << ' ' << result.errorSqlState << ' ' << result.errorMessage << std::endl;
            result.throwIfFailed();
            assert(result.structuredRowsAvailable && result.structuredRows == rows);
            assert(prepared->legacySql() == original); // no shared AST/routine-name mutation
            ++checked;
        }
    };
    check("SELECT count(*) FROM a", {{"3"}}, {"bigint"});
    check("SELECT (SELECT count(v) FROM a)", {{"2"}}, {"bigint"});
    check("SELECT (SELECT sum(v) FROM a)", {{"9007199254740995"}}, {"numeric"});
    check("SELECT (SELECT sum(id) FROM a)", {{"6"}}, {"bigint"});
    check("SELECT (SELECT count(DISTINCT t) FROM a)", {{"2"}}, {"bigint"});
    check("SELECT (SELECT count(*) FILTER(WHERE b) FROM a)", {{"1"}}, {"bigint"});
    check("SELECT (SELECT bool_and(b) FROM a)", {{"f"}}, {"boolean"});
    check("SELECT (SELECT bool_or(b) FROM a)", {{"t"}}, {"boolean"});
    check("SELECT (SELECT count(*) FROM a WHERE false)", {{"0"}}, {"bigint"});
    check("SELECT (SELECT count(*)+count(v) FROM a)", {{"5"}}, {"bigint"});
    check("SELECT (SELECT CASE WHEN false THEN sum(1/0) ELSE 1 END FROM a)", {{"1"}}, {"bigint"});
    check("SELECT r.id,(SELECT count(*) FROM a q WHERE q.id<=r.id) FROM a r ORDER BY r.id",
          {{"1","1"},{"2","2"},{"3","3"}}, {"integer","bigint"});
    for (const auto& invalid : {
         "SELECT (SELECT sum(t) FROM a WHERE false)",
         "SELECT (SELECT id+count(*) FROM a)",
         "SELECT (SELECT count(count(*)) FROM a)",
         "SELECT (SELECT count(*) FROM a WHERE count(*)>0)"}) {
        std::string state;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db, invalid));
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine, db, prepared, prepared->ast.get()));
            result.throwIfFailed();
        } catch (const DbError& error) { state = error.sqlState(); }
        assert(state == (std::string(invalid).find("sum(t)") != std::string::npos ? "42883" : "42803"));
        ++checked;
    }
    auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db, "SELECT (SELECT sum(v) FROM a WHERE false)"));
    auto cursor = QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,prepared,prepared->ast.get()),prepared->output);
    std::vector<ExprValue> row;
    assert(cursor->next(row) && row.size() == 1 && row.front().isNull && row.front().typeName == "numeric");
    assert(!cursor->next(row)); cursor->close(); ++checked;
    std::cout << "[PREPARED GLOBAL AGGREGATE] actual graph/typed NULL/exact BIGINT/repeated execution/correlation checks=" << checked << '\n';
}
