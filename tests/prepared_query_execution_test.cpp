#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name = "prepared_query_execution";
    const auto db = testDbPath(name);
    cleanupTestDb(name);
    assert(engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema schema; schema.len = 3;
    schema.cols[0].dataName = "id"; schema.cols[0].dataType = "int"; schema.cols[0].dsize = 4;
    schema.cols[1].dataName = "ID"; schema.cols[1].dataType = "bigint"; schema.cols[1].dsize = 8;
    schema.cols[2].dataName = "payload"; schema.cols[2].dataType = "text"; schema.cols[2].dsize = 128;
    assert(engine.createTable(db, "runtime_rows", schema) == DBStatus::OK);
    auto query = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT a.id,a.\"ID\",(SELECT a.id),(SELECT a.\"ID\"),"
        "CASE WHEN false THEN(SELECT 1/0) ELSE a.id END FROM runtime_rows a"));
    auto* select = static_cast<SelectStmt*>(query->ast.get());
    PreparedQueryExecution execution(query, &engine, db);
    for (auto& target : select->selectList) execution.prepareExpression(target.expr.get());
    const auto* bound = static_cast<ColumnRefExpr*>(select->selectList[0].expr.get());
    assert(bound->binding);
    const auto occurrence = bound->binding->sourceOrdinal;
    auto row = execution.context();
    execution.setSourceRow(row, occurrence, {{"integer","7"},{"bigint","2147483648"},{"text"," O'Brien "}});
    assert(execution.evaluate(select->selectList[0].expr.get(), row).value == "7");
    assert(execution.evaluate(select->selectList[1].expr.get(), row).value == "2147483648");
    assert(execution.evaluate(select->selectList[2].expr.get(), row).value == "7");
    const auto wide = execution.evaluate(select->selectList[3].expr.get(), row);
    assert(wide.value == "2147483648" && wide.typeName == "bigint");
    assert(execution.evaluate(select->selectList[4].expr.get(), row).value == "7");
    // Declared AST types beat missing or deliberately conflicting text hints.
    assert(ExprHelper::inferParsedResultType(select->selectList[1].expr.get(),
        {{"ID","integer"}}, db, &engine) == "bigint");
    bool rejected = false;
    try { (void)execution.evaluate(select->selectList[0].expr.get(), execution.context()); }
    catch (const DbError& error) { rejected = error.sqlState() == "XX000"; }
    assert(rejected); // no case-folded string-map fallback
    execution.setSourceRow(row, occurrence, {{"integer","8"},{"bigint","9"},{"text","",true}});
    assert(execution.evaluate(select->selectList[2].expr.get(), row).value == "8");

    for (const auto& text : {std::string(""),std::string("null"),std::string("  a b  "),std::string("O'Brien")}) {
        auto prepared = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
            "SELECT (SELECT a.payload) FROM runtime_rows a"));
        auto* statement = static_cast<SelectStmt*>(prepared->ast.get());
        auto* child = statement->selectList[0].expr.get();
        auto* childStmt = static_cast<SelectStmt*>(child->preparedSubquery.get());
        const auto sid = static_cast<ColumnRefExpr*>(childStmt->selectList[0].expr.get())->binding->sourceOrdinal;
        PreparedQueryExecution runtime(prepared, &engine, db); runtime.prepareExpression(child);
        auto cells = runtime.context();
        runtime.setSourceRow(cells, sid, {{"integer","1"},{"bigint","2"},{"text",text}});
        auto value = runtime.evaluate(child, cells);
        assert(value.typeName == "text" && !value.isNull && value.value == text);
        runtime.setSourceRow(cells, sid, {{"integer","1"},{"bigint","2"},{"text","",true}});
        value = runtime.evaluate(child, cells);
        assert(value.typeName == "text" && value.isNull);
    }
    auto param = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT (SELECT p)", {{"param:p","p","bigint",{},true,"2147483648",1}}));
    auto* parameterChild = static_cast<SelectStmt*>(param->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution parameterRuntime(param, &engine, db);
    parameterRuntime.prepareExpression(parameterChild);
    assert(parameterRuntime.evaluate(parameterChild, parameterRuntime.context()).value == "2147483648");
    auto extractQuery = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT EXTRACT(YEAR FROM DATE '2025-01-02')"));
    auto* extractExpr = static_cast<SelectStmt*>(extractQuery->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution grammar(extractQuery,&engine,db); grammar.prepareExpression(extractExpr);
    const auto year = grammar.evaluate(extractExpr,grammar.context());
    assert(!year.isNull && year.value == "2025");
    auto zeroQuery = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT (SELECT id FROM runtime_rows)"));
    auto* zeroChild = static_cast<SelectStmt*>(zeroQuery->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution zero(zeroQuery,&engine,db); zero.prepareExpression(zeroChild);
    const auto zeroValue = zero.evaluate(zeroChild,zero.context());
    assert(zeroValue.isNull && zeroValue.typeName == "integer");
    assert(engine.createUDF(db,"runtime_identity",{"arg"},{"integer"},
        "BEGIN RETURN arg; END;",'v',"plpgsql","integer") == DBStatus::OK);
    auto reused = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT runtime_identity(4)"));
    auto* reusedExpr = static_cast<SelectStmt*>(reused->ast.get())->selectList[0].expr.get();
    for (size_t repeat = 0; repeat < 2; ++repeat) {
        PreparedQueryExecution invocation(reused,&engine,db);
        invocation.prepareExpression(reusedExpr);
        assert(invocation.evaluate(reusedExpr,invocation.context()).value == "4");
        assert(static_cast<FunctionCallExpr*>(reusedExpr)->funcName == "runtime_identity");
    }

    auto joins = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT id,\"A\".id,\"a\".\"ID\",(SELECT id) FROM runtime_rows AS \"A\" JOIN runtime_rows AS \"a\" USING(id)"));
    auto* joinSelect = static_cast<SelectStmt*>(joins->ast.get());
    PreparedQueryExecution joinRuntime(joins, &engine, db);
    for (auto& target : joinSelect->selectList) joinRuntime.prepareExpression(target.expr.get());
    const auto* merged = static_cast<ColumnRefExpr*>(joinSelect->selectList[0].expr.get());
    const auto* left = static_cast<ColumnRefExpr*>(joinSelect->selectList[1].expr.get());
    const auto* right = static_cast<ColumnRefExpr*>(joinSelect->selectList[2].expr.get());
    assert(merged->binding->mergedUsing && merged->binding->sourceOrdinal != left->binding->sourceOrdinal);
    assert(left->binding->sourceOrdinal != right->binding->sourceOrdinal);
    auto joinRow = joinRuntime.context();
    joinRuntime.setSourceRow(joinRow, merged->binding->sourceOrdinal, {{"integer","10"}});
    joinRuntime.setSourceRow(joinRow, left->binding->sourceOrdinal, {{"integer","11"},{"bigint","12"},{"text","L"}});
    joinRuntime.setSourceRow(joinRow, right->binding->sourceOrdinal, {{"integer","21"},{"bigint","22"},{"text","R"}});
    assert(joinRuntime.evaluate(joinSelect->selectList[0].expr.get(), joinRow).value == "10");
    assert(joinRuntime.evaluate(joinSelect->selectList[1].expr.get(), joinRow).value == "11");
    assert(joinRuntime.evaluate(joinSelect->selectList[2].expr.get(), joinRow).value == "22");
    assert(joinRuntime.evaluate(joinSelect->selectList[3].expr.get(), joinRow).value == "10");

    // A unique ordinal cannot grant access to a sibling SQL namespace.
    auto invalid = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT(SELECT id FROM runtime_rows b),(SELECT id FROM runtime_rows c)"));
    auto* invalidSelect = static_cast<SelectStmt*>(invalid->ast.get());
    auto* firstInner = static_cast<ColumnRefExpr*>(static_cast<SelectStmt*>(invalidSelect->selectList[0].expr->preparedSubquery.get())->selectList[0].expr.get());
    auto* secondInner = static_cast<ColumnRefExpr*>(static_cast<SelectStmt*>(invalidSelect->selectList[1].expr->preparedSubquery.get())->selectList[0].expr.get());
    firstInner->binding = secondInner->binding;
    rejected = false;
    try { PreparedQueryExecution forbidden(invalid, &engine, db); }
    catch (const DbError& error) { rejected = error.sqlState() == "XX000"; }
    assert(rejected);

    for (const std::string value : {"1","2","3"})
        assert(engine.insert(db,"runtime_rows",{{"id",value},{"ID",value},{"payload",value}}) == DBStatus::OK);
    auto multi = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT(SELECT id FROM runtime_rows)"));
    auto* multiple = static_cast<SelectStmt*>(multi->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution cardinality(multi,&engine,db); cardinality.prepareExpression(multiple);
    rejected = false;
    try { (void)cardinality.evaluate(multiple, cardinality.context()); }
    catch (const DbError& error) { rejected = error.sqlState() == "21000"; }
    assert(rejected);
    auto badCast = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT(SELECT CAST('bad' AS INTEGER))"));
    auto* badCastExpr = static_cast<SelectStmt*>(badCast->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution conversion(badCast,&engine,db); conversion.prepareExpression(badCastExpr);
    rejected = false;
    try { (void)conversion.evaluate(badCastExpr,conversion.context()); }
    catch (const DbError& error) { rejected = error.sqlState() == "22P02"; }
    assert(rejected);

    assert(engine.beginTransaction(db) == DBStatus::OK);
    assert(engine.beginSqlCommand());
    const auto cid = engine.currentCommandId();
    const auto xid = engine.currentTxnId();
    const auto view = *engine.getCurrentReadView();
    size_t calls = 0;
    bool errorMode = false;
    std::string expectedText;
    engine.setPlpgsqlQueryExecutor([&](const std::string& database, const std::string& sql,
                                    const PlPgsqlQueryOptions& options) {
        assert(database == db && options.maxRows == 2 &&
            options.purpose == PlPgsqlQueryOptions::Purpose::OrdinarySubquery);
        assert(engine.currentCommandId() == cid && engine.currentTxnId() == xid);
        assert(engine.getCurrentReadView()->activeTxnIds == view.activeTxnIds);
        assert(engine.getCurrentReadView()->upLimitId == view.upLimitId);
        ++calls;
        PlPgsqlQueryResult result; result.ok = !errorMode;
        result.columnCount = 1; result.rowCount = 1;
        result.columnTypes = {"integer"}; result.firstRow = {std::to_string(calls)};
        if (errorMode) { result.sqlState = "P0002"; result.message = "strict no row"; }
        if (!expectedText.empty()) assert(sql.find(expectedText) != std::string::npos);
        return result;
    });
    auto memoQuery = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT (SELECT abs(1)),(SELECT abs(1)),CASE WHEN false THEN(SELECT abs(2)) ELSE 9 END"));
    auto* memoSelect = static_cast<SelectStmt*>(memoQuery->ast.get());
    PreparedQueryExecution memo(memoQuery, &engine, db);
    for (auto& target : memoSelect->selectList) memo.prepareExpression(target.expr.get());
    assert(memo.evaluate(memoSelect->selectList[0].expr.get(), memo.context()).value == "1");
    assert(memo.evaluate(memoSelect->selectList[0].expr.get(), memo.context()).value == "1" && calls == 1);
    assert(memo.evaluate(memoSelect->selectList[1].expr.get(), memo.context()).value == "2" && calls == 2);
    assert(memo.evaluate(memoSelect->selectList[2].expr.get(), memo.context()).value == "9" && calls == 2);
    PreparedQueryExecution nextStatement(memoQuery,&engine,db);
    nextStatement.prepareExpression(memoSelect->selectList[0].expr.get());
    assert(nextStatement.evaluate(memoSelect->selectList[0].expr.get(),nextStatement.context()).value == "3");
    assert(calls == 3); // no result cache across executions of the same AST
    // Errors propagate unchanged and are never replaced by a cached NULL.
    errorMode = true;
    auto failQuery = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT (SELECT abs(3))"));
    auto* failed = static_cast<SelectStmt*>(failQuery->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution failure(failQuery, &engine, db); failure.prepareExpression(failed);
    for (size_t repeat = 0; repeat < 2; ++repeat) {
        rejected = false;
        try { (void)failure.evaluate(failed, failure.context()); }
        catch (const DbError& error) { rejected = error.sqlState() == "P0002"; }
        assert(rejected);
    }
    assert(calls == 5);
    errorMode = false;

    // Retain scope through CTE/derived ancestors; replacing outer cells must
    // also preserve the unaliased CTE output column name.
    auto ancestors = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT(SELECT q.id FROM(WITH c AS(SELECT a.id)SELECT c.id FROM c)q)FROM runtime_rows a"));
    auto* ancestorSelect = static_cast<SelectStmt*>(ancestors->ast.get());
    auto* ancestorChild = ancestorSelect->selectList[0].expr.get();
    PreparedQueryExecution ancestorRuntime(ancestors, &engine, db); ancestorRuntime.prepareExpression(ancestorChild);
    auto ancestorRow = ancestorRuntime.context();
    size_t outerOrdinal = 0;
    for (const auto& source : ancestors->sourceRanges) if (source.owner == ancestorSelect && source.source == ancestorSelect->fromClause.get()) outerOrdinal = source.ordinal;
    ancestorRuntime.setSourceRow(ancestorRow, outerOrdinal, {{"integer","12"},{"bigint","13"},{"text","z"}});
    expectedText = "CAST('12' AS integer) AS \"id\"";
    (void)ancestorRuntime.evaluate(ancestorChild, ancestorRow);
    assert(calls == 6);
    expectedText.clear();
    // Internally correlated children are still initplans of the outer query.
    auto internal = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT(SELECT(SELECT b.id) FROM runtime_rows b)"));
    auto* internalChild = static_cast<SelectStmt*>(internal->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution internalRuntime(internal, &engine, db); internalRuntime.prepareExpression(internalChild);
    (void)internalRuntime.evaluate(internalChild, internalRuntime.context());
    (void)internalRuntime.evaluate(internalChild, internalRuntime.context());
    assert(calls == 7);
    assert(engine.currentCommandId() == cid && engine.currentTxnId() == xid);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    engine.setPlpgsqlQueryExecutor({});
    cleanupTestDb(name);
    std::cout << "[PREPARED QUERY EXECUTION] passed\n";
}
