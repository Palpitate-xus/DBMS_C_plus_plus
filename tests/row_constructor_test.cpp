#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
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
    const auto nullEqual = ExprHelper::evalString("ROW(1,NULL)=ROW(1,NULL)",{});
    assert(nullEqual.ok && nullEqual.isNull);
    const auto nullIn = ExprHelper::evalString("ROW(1,NULL) IN(ROW(1,NULL),ROW(2,1))",{});
    assert(nullIn.ok && nullIn.isNull);
    SQLParser parser;
    ExprEvaluator direct;
    for (const auto* sql : {"SELECT ROW(1,NULL)=ROW(1,NULL)",
         "SELECT ROW(1,NULL) IN(ROW(1,NULL),ROW(2,1))"}) {
        const auto parsed = parser.parse(sql);
        assert(parsed.success);
        const auto* select = static_cast<const SelectStmt*>(parsed.stmt.get());
        assert(direct.eval(select->selectList.front().expr.get(),RowContext{}).isNull);
    }
    for (const auto* sql : {"SELECT ROW(1,'a')","SELECT (1,'a')","SELECT ROW()"}) {
        const auto parsed = parser.parse(sql);
        assert(parsed.success);
        const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        const auto* row = dynamic_cast<const RowExpr*>(select->selectList.front().expr.get());
        assert(row && row->constructor);
    }
    const auto parsed = parser.parse("SELECT ROW(1) IN(ROW(1),ROW(2))");
    assert(parsed.success);
    const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    const auto* binary = dynamic_cast<const BinaryOpExpr*>(select->selectList.front().expr.get());
    assert(binary);
    assert(static_cast<const RowExpr*>(binary->left.get())->constructor);
    assert(!static_cast<const RowExpr*>(binary->right.get())->constructor);
    const auto sublink = parser.parse("SELECT (a,b) NOT IN(SELECT a,b FROM t)");
    assert(sublink.success);
    const auto* subquerySelect = static_cast<const SelectStmt*>(sublink.stmt.get());
    const auto* subqueryComparison = dynamic_cast<const BinaryOpExpr*>(subquerySelect->selectList.front().expr.get());
    assert(subqueryComparison && subqueryComparison->right->type == ExprType::Subquery);

    const auto db = testDbPath("row_constructor");
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
    const auto previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    size_t checked = 0;
    const auto check = [&](const std::string& sql,const std::vector<std::vector<std::string>>& expected) {
        std::cout << "ROW_NATIVE_SQL " << sql << std::endl;
        auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        const auto source = query->legacySql();
        for (int run = 0; run < 2; ++run) {
            auto result = QueryPlanner::executePlanChecked(
                QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()));
            if (!result.ok) std::cout << "ROW_NATIVE_ERROR " << result.errorSqlState << ' ' << result.errorMessage << std::endl;
            result.throwIfFailed();
            assert(result.structuredRowsAvailable && result.structuredRows == expected);
            assert(query->legacySql() == source);
            ++checked;
        }
    };
    check("SELECT ROW(1,'a'),ROW(),ROW(NULL,'')",{{"(1,a)","()","(,\"\")"}});
    check("SELECT (1,'a')",{{"(1,a)"}});
    check("SELECT ROW(ROW(1,'a'),ARRAY[1,2])",{{"(\"(1,a)\",\"{1,2}\")"}});
    check("SELECT CASE WHEN true THEN ROW(1,'a') ELSE ROW(2,'b') END",{{"(1,a)"}});
    check("SELECT ROW(1,2)<ROW(2,1),ROW(2,1)>ROW(1,2)",{{"t","t"}});
    check("SELECT ROW(1,'a')=ROW(1,'a'),ROW(1,'2')=ROW(1,2)",{{"t","t"}});
    check("SELECT ROW(NULL,2)=ROW(NULL,3),ROW(NULL,2)<>ROW(NULL,3)",{{"f","t"}});
    check("SELECT ROW(NULL,1) IS NULL,ROW(NULL,1) IS NOT NULL,ROW() IS NULL,ROW() IS NOT NULL",{{"f","f","t","t"}});
    check("SELECT ROW(1) IN(ROW(1),ROW(2))",{{"t"}});
    check("SELECT (SELECT min(ROW(1,'a')))",{{"(1,a)"}});
    check("SELECT (SELECT min(v) FROM(VALUES(ROW(2,'b')),(ROW(1,'a'))) AS t(v))",{{"(1,a)"}});
    check("SELECT (SELECT min(v) FROM(VALUES(ROW(1,NULL::INT)),(ROW(1,2))) AS t(v))",{{"(1,2)"}});
    check("SELECT min(v) FROM(VALUES(ROW(NULL::INT)),(ROW(NULL::INT))) AS t(v)",{{"()"}});
    check("SELECT ROW(ROW(NULL::INT))=ROW(ROW(NULL::INT))",{{"t"}});
    check("SELECT min(v) FROM(VALUES(ROW(1,NULL)),(ROW(2,NULL))) AS t(v)",{{"(1,)"}});
    check("SELECT ROW(ROW(1,NULL))<ROW(ROW(2,NULL))",{{"t"}});
    check("SELECT ROW(1)::record, ROW(1)::TEXT, ROW(1)::VARCHAR(8), ROW(1)::CHAR(8), ROW(1)::NAME",
          {{"(1)","(1)","(1)","(1)     ","(1)"}});
    check("SELECT '(1)'::TEXT::record WHERE false",{});
    check("SELECT CAST('(1)'::TEXT AS record) WHERE false",{});
    for (const auto* sql : {"SELECT '(1)'::TEXT::record WHERE false",
         "SELECT CAST('(1)'::TEXT AS record) WHERE false"}) {
        auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        PreparedQueryExecution execution(prepared,&g_engine,db);
        execution.planStatementConstants(prepared->ast.get());
        ++checked;
    }
    for (const auto& invalid : {std::pair<const char*,const char*>{"SELECT ROW(1)=ROW(1,2)","42601"},
         {"SELECT ROW()=ROW()","0A000"},{"SELECT \"ROW\"(1)","42883"},
         {"SELECT pg_catalog.ROW(1)","42883"},
         {"SELECT ROW(1)::INT WHERE false","42846"},
         {"SELECT ROW(1)::JSON WHERE false","42846"},
         {"SELECT NULL::INT::record WHERE false","42846"},
         {"SELECT '(1)'::record WHERE false","0A000"}}) {
        std::string state;
        try { (void)g_engine.prepareBoundQuery(db,invalid.first); }
        catch (const DbError& error) { state = error.sqlState(); }
        assert(state == invalid.second);
        ++checked;
    }
    for (const auto* sql : {"SELECT min(v) FROM(VALUES(ROW(NULL)),(ROW(NULL))) AS t(v)",
         "SELECT ROW(ROW(NULL))=ROW(ROW(NULL))", "SELECT '(1)'::TEXT::record"}) {
        std::string state;
        try {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
            QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
                &g_engine,db,prepared,prepared->ast.get())).throwIfFailed();
        } catch (const DbError& error) { state = error.sqlState(); }
        assert(state == (std::string(sql).find("::TEXT::record") != std::string::npos ? "0A000" : "42883"));
        ++checked;
    }
    auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT ROW(1,'a',NULL)"));
    assert(query->output.size() == 1 && query->output[0].typeOid == 2249);
    auto cursor = QueryPlanner::makePreparedCursor(
        QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()),query->output);
    std::vector<ExprValue> values;
    assert(cursor->next(values) && values.size() == 1 && values[0].typeName == "record");
    assert(values[0].recordFields && values[0].recordFields->size() == 3);
    assert((*values[0].recordFields)[0].typeName == "integer");
    assert((*values[0].recordFields)[1].typeName == "unknown");
    assert((*values[0].recordFields)[2].isNull);
    assert(!cursor->next(values)); cursor->close(); ++checked;
    std::cout << "[ROW CONSTRUCTOR] role/typed fields/record output/repeated graph checks=" << checked << '\n';
}
