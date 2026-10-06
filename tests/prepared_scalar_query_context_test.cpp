#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <map>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name = "prepared_scalar_query_context";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table; table.len = 1;
    table.cols[0].dataName = "id"; table.cols[0].dataType = "int";
    table.cols[0].dsize = 4;
    assert(engine.createTable(db, "preparation_writes", table) == DBStatus::OK);
    assert(engine.createTable(db, "native_rows", table) == DBStatus::OK);
    for (const std::string value : {"1", "2", "3"})
        assert(engine.insert(db, "native_rows", {{"id", value}}) == DBStatus::OK);
    assert(engine.createUDF(db, "PreparedWide", {"arg"}, {"bigint"},
        "BEGIN INSERT INTO preparation_writes VALUES(1); RETURN arg; END;",
        'v', "plpgsql", "bigint") == DBStatus::OK);
    assert(engine.beginTransaction(db) == DBStatus::OK);
    assert(engine.beginSqlCommand());
    const auto cid = engine.currentCommandId();
    const auto xid = engine.currentTxnId();
    const auto view = *engine.getCurrentReadView();
    auto prepared = engine.prepareBoundQuery(db,
        "SELECT CASE WHEN false THEN (SELECT public.\"PreparedWide\"(7)) ELSE 9 END");
    assert(engine.inTransaction() && engine.currentTxnId() == xid && engine.currentCommandId() == cid);
    assert(engine.getCurrentReadView()->currentCommandId == view.currentCommandId);
    assert(engine.getCurrentReadView()->creatorTxnId == view.creatorTxnId);
    assert(engine.getCurrentReadView()->upLimitId == view.upLimitId);
    assert(engine.getCurrentReadView()->lowLimitId == view.lowLimitId);
    assert(engine.getCurrentReadView()->activeTxnIds == view.activeTxnIds);
    assert(engine.query(db, "preparation_writes", {}, {}, {}).empty());
    auto* select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    assert(select && select->selectList.size() == 1);
    ExprEvaluator evaluator; evaluator.setCurrentDB(db);
    RowContext context;
    size_t callbacks = 0;
    evaluator.setScalarSubqueryExecutor([&](const Expr* child, const RowContext&) {
        ++callbacks;
        assert(child->preparedSubquery);
        return ExprValue("bigint", "7");
    });
    assert(evaluator.eval(select->selectList.front().expr, context).value == "9");
    assert(callbacks == 0);

    prepared = engine.prepareBoundQuery(db, "SELECT (SELECT public.\"PreparedWide\"(7))");
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    const auto* literal = dynamic_cast<LiteralExpr*>(select->selectList.front().expr.get());
    assert(literal && literal->preparedSubquery && literal->typeName == "bigint");
    assert(engine.query(db, "preparation_writes", {}, {}, {}).empty());
    assert(evaluator.eval(literal, context).value == "7" && callbacks == 1);
    ExprEvaluator withoutContext;
    bool rejected = false;
    try { (void)withoutContext.eval(literal, context); }
    catch (const DbError& error) { rejected = error.sqlState() == "0A000"; }
    assert(rejected);

    // Real occurrence/descriptor bindings stay distinct through aliases,
    // ancestors and the virtual JOIN USING merged namespace.
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& relation) {
        return QueryRelationMetadata{"public", relation, {{"id","integer"},{"ID","bigint"}}, {}};
    };
    prepared = prepareQuery("SELECT a.id,(SELECT a.id),(SELECT a.\"ID\" FROM t AS a) FROM t AS a", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    const auto* local = dynamic_cast<ColumnRefExpr*>(select->selectList[0].expr.get());
    const auto* correlated = dynamic_cast<SelectStmt*>(select->selectList[1].expr->preparedSubquery.get());
    const auto* outer = dynamic_cast<ColumnRefExpr*>(correlated->selectList[0].expr.get());
    const auto* shadow = dynamic_cast<SelectStmt*>(select->selectList[2].expr->preparedSubquery.get());
    const auto* inner = dynamic_cast<ColumnRefExpr*>(shadow->selectList[0].expr.get());
    assert(local->binding && outer->binding && inner->binding);
    assert(local->binding->scopeDepth == 0 && outer->binding->scopeDepth == 1);
    assert(local->binding->sourceOrdinal == outer->binding->sourceOrdinal);
    assert(inner->binding->scopeDepth == 0 && inner->binding->columnOrdinal == 1);
    assert(inner->binding->sourceOrdinal != outer->binding->sourceOrdinal);
    assert(inner->binding->declaredType == "bigint");
    prepared = prepareQuery("SELECT (SELECT q.v FROM(WITH c AS(SELECT a.id AS v) SELECT c.v FROM c)q) FROM t a", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    const auto* child = dynamic_cast<SelectStmt*>(select->selectList[0].expr->preparedSubquery.get());
    const auto* withQuery = dynamic_cast<SelectStmt*>(child->fromClause->subquery.get());
    const auto* cte = dynamic_cast<SelectStmt*>(withQuery->ctes[0].query.get());
    const auto* cteOuter = dynamic_cast<ColumnRefExpr*>(cte->selectList[0].expr.get());
    assert(cteOuter->binding && cteOuter->binding->scopeDepth == 3);
    prepared = prepareQuery("SELECT id,a.id,b.\"ID\" FROM t a JOIN t b USING(id)", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    const auto* merged = dynamic_cast<ColumnRefExpr*>(select->selectList[0].expr.get());
    const auto* left = dynamic_cast<ColumnRefExpr*>(select->selectList[1].expr.get());
    const auto* right = dynamic_cast<ColumnRefExpr*>(select->selectList[2].expr.get());
    assert(merged->binding && merged->binding->mergedUsing && merged->binding->columnOrdinal == 0);
    assert(left->binding && right->binding && left->binding->sourceOrdinal != right->binding->sourceOrdinal);
    assert(merged->binding->sourceOrdinal != left->binding->sourceOrdinal);
    assert(merged->binding->sourceOrdinal != right->binding->sourceOrdinal);
    assert(prepared.sourceRanges[merged->binding->sourceOrdinal].mergedUsing);
    assert(prepared.sourceRanges[left->binding->sourceOrdinal].owner == select);
    prepared = prepareQuery("SELECT \"A\".id,\"a\".id FROM t AS \"A\" JOIN t AS \"a\" ON \"A\".id=\"a\".id", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    left = dynamic_cast<ColumnRefExpr*>(select->selectList[0].expr.get());
    right = dynamic_cast<ColumnRefExpr*>(select->selectList[1].expr.get());
    assert(left->binding && right->binding && left->binding->sourceOrdinal != right->binding->sourceOrdinal);
    const auto* on = dynamic_cast<BinaryOpExpr*>(select->fromClause->joinCondition.get());
    const auto* onLeft = dynamic_cast<ColumnRefExpr*>(on->left.get());
    const auto* onRight = dynamic_cast<ColumnRefExpr*>(on->right.get());
    assert(onLeft->binding->sourceOrdinal == left->binding->sourceOrdinal);
    assert(onRight->binding->sourceOrdinal == right->binding->sourceOrdinal);
    prepared = prepareQuery("UPDATE t AS a SET id=b.id FROM t AS b WHERE a.id=b.id RETURNING a.id", {}, metadata);
    const auto* update = dynamic_cast<UpdateStmt*>(prepared.ast.get());
    const auto* target = dynamic_cast<ColumnRefExpr*>(update->returning[0].expr.get());
    const auto* source = dynamic_cast<ColumnRefExpr*>(update->setClauses.at("id").get());
    assert(target->binding && source->binding && target->binding->sourceOrdinal != source->binding->sourceOrdinal);
    assert(!prepared.sourceRanges[target->binding->sourceOrdinal].source);
    assert(prepared.sourceRanges[source->binding->sourceOrdinal].source);
    prepared = prepareQuery("SELECT a.id AS label FROM t a ORDER BY label", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    const auto* alias = dynamic_cast<ColumnRefExpr*>(select->orderBy[0].expr.get());
    assert(alias && !alias->binding); // output alias is not a physical datum
    prepared = prepareQuery("SELECT (SELECT 1),(SELECT 1)", {}, metadata);
    select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    assert(select->selectList[0].expr.get() != select->selectList[1].expr.get());
    assert(select->selectList[0].expr->preparedSubquery.get() != select->selectList[1].expr->preparedSubquery.get());

    // The checked native scalar child preserves actual cell bytes and NULL.
    for (const auto& value : {std::string(""), std::string("null"), std::string("a b"), std::string("O'Brien")}) {
        std::string quoted = "'";
        for (char byte : value) { quoted += byte; if (byte == '\'') quoted += '\''; }
        quoted += "'::text";
        const auto result = engine.executeScalarSubquery(db, "SELECT " + quoted);
        assert(!result.isNull && result.typeName == "text" && result.value == value);
    }
    const auto null = engine.executeScalarSubquery(db, "SELECT CAST(NULL AS text)");
    assert(null.isNull && null.typeName == "text");
    const auto empty = engine.executeScalarSubquery(db, "SELECT id FROM preparation_writes");
    assert(empty.isNull && empty.typeName == "integer");
    assert(engine.currentCommandId() == cid);
    for (const auto& [sql, state] : std::map<std::string, std::string>{
        {"SELECT id FROM native_rows", "21000"}, {"SELECT 1,2", "42601"}}) {
        rejected = false;
        try { (void)engine.executeScalarSubquery(db, sql); }
        catch (const DbError& error) { rejected = error.sqlState() == state; }
        assert(rejected);
    }
    const auto callerView = *engine.getCurrentReadView();

    size_t dispatched = 0;
    engine.setPlpgsqlQueryExecutor([&](const std::string& database, const std::string& sql,
                                    const PlPgsqlQueryOptions& options) {
        assert(database == db && options.maxRows == 2);
        assert(options.purpose == PlPgsqlQueryOptions::Purpose::OrdinarySubquery);
        assert(engine.currentCommandId() == cid && engine.currentTxnId() == xid);
        assert(engine.getCurrentReadView()->upLimitId == callerView.upLimitId);
        assert(engine.getCurrentReadView()->lowLimitId == callerView.lowLimitId);
        assert(engine.getCurrentReadView()->activeTxnIds == callerView.activeTxnIds);
        ++dispatched;
        PlPgsqlQueryResult result; result.ok = true;
        result.columnCount = 1; result.rowCount = 1;
        result.columnTypes = {"text"}; result.firstRow = {std::string(" null ")};
        if (sql == "SELECT 1,2") result.columnCount = 2;
        if (sql == "SELECT 1 FROM two_rows") result.rowCount = 2;
        if (sql == "SELECT broken()") { result.ok = false; result.sqlState = "P0002"; result.message = "no rows"; }
        return result;
    });
    assert(engine.executeScalarSubquery(db, "SELECT ' null '").value == " null ");
    for (const auto& [sql, state] : std::map<std::string, std::string>{
        {"SELECT 1,2", "42601"}, {"SELECT 1 FROM two_rows", "21000"},
        {"SELECT broken()", "P0002"}, {"INSERT INTO preparation_writes VALUES(9)", "42601"}}) {
        rejected = false;
        try { (void)engine.executeScalarSubquery(db, sql); }
        catch (const DbError& error) { rejected = error.sqlState() == state; }
        assert(rejected);
    }
    assert(dispatched == 4 && engine.currentCommandId() == cid);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    engine.setPlpgsqlQueryExecutor({});
    cleanupTestDb(name);
    std::cout << "[PREPARED SCALAR QUERY CONTEXT] passed\n";
}
