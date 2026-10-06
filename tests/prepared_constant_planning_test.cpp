#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name = "prepared_constant_planning";
    cleanupTestDb(name);
    const auto db = testDbPath(name);
    assert(engine.createDatabase(db,"utf8") == DBStatus::OK);
    TableSchema schema; schema.len = 1;
    schema.cols[0].dataName = "id"; schema.cols[0].dataType = "int"; schema.cols[0].dsize = 4;
    assert(engine.createTable(db,"plan_constant_rows",schema) == DBStatus::OK);
    assert(engine.insert(db,"plan_constant_rows",{{"id","1"}}) == DBStatus::OK);
    assert(engine.createSequence(db,"plan_constant_effects",1,1) == DBStatus::OK);
    assert(engine.createUDF(db,"plan_constant_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('plan_constant_effects'); RETURN p; END;",'v',"plpgsql","integer") == DBStatus::OK);
    const std::vector<std::pair<std::string,std::string>> cases = {
        {"SELECT 1/0","22012"},
        {"SELECT 1/0 WHERE false","22012"},
        {"SELECT 1/0 LIMIT 0","22012"},
        {"SELECT(SELECT 1/0) LIMIT 0","22012"},
        {"SELECT NULL::INT/0",""},
        {"SELECT CAST(2147483648 AS INT)","22003"},
        {"SELECT CASE WHEN false THEN 1/0 ELSE 2 END",""},
        {"SELECT CASE WHEN true THEN 2 ELSE 1/0 END",""},
        {"SELECT CASE WHEN id=1 THEN 1/0 ELSE 2 END FROM plan_constant_rows","22012"},
        {"SELECT CASE WHEN false THEN(SELECT 1/0) ELSE 2 END",""},
        {"SELECT CASE WHEN true THEN(SELECT 1/0) ELSE 2 END","22012"},
        {"SELECT 1 WHERE false AND 1/0=0",""},
        {"SELECT 1 WHERE true OR 1/0=0",""},
        {"SELECT 1 FROM plan_constant_rows WHERE id=1 AND 1/0=0","22012"},
        {"SELECT plan_constant_writer(1) WHERE false",""},
        {"SELECT plan_constant_writer(1/0) WHERE false","22012"},
        {"SELECT(SELECT plan_constant_writer(1))",""},
        {"SELECT CASE WHEN false THEN(SELECT plan_constant_writer(1)) ELSE 2 END",""},
        {"WITH unused AS(SELECT 1/0) SELECT 2",""},
        {"WITH reached AS(SELECT 1/0 AS n) SELECT n FROM reached","22012"},
        {"WITH a AS(SELECT 1/0 AS n), b AS(SELECT n FROM a) SELECT n FROM b","22012"},
        {"WITH unused AS(SELECT plan_constant_writer(1)) SELECT 2",""},
        {"WITH ins AS(INSERT INTO plan_constant_rows VALUES(1/0) RETURNING id) SELECT 2","22012"},
        {"WITH ins AS(INSERT INTO plan_constant_rows VALUES(plan_constant_writer(1)) RETURNING id) SELECT 2",""},
        {"UPDATE plan_constant_rows SET id=1/0 WHERE false","22012"},
        {"UPDATE plan_constant_rows SET id=id/0 WHERE false",""},
        {"UPDATE plan_constant_rows SET id=(SELECT 1/0) WHERE false","22012"},
        {"UPDATE plan_constant_rows SET id=CASE WHEN false THEN(SELECT 1/0) ELSE 2 END",""},
        {"DELETE FROM plan_constant_rows WHERE 1/0=0","22012"},
        {"INSERT INTO plan_constant_rows SELECT 1/0 WHERE false","22012"},
        {"SELECT 1 FROM(SELECT 1/0 AS n)q",""},
        {"SELECT n FROM(SELECT 1/0 AS n)q","22012"},
        {"WITH c AS(SELECT 1/0 AS n) SELECT 1 FROM c",""},
        {"WITH c AS MATERIALIZED(SELECT 1/0 AS n) SELECT 1 FROM c","22012"},
        {"WITH c AS NOT MATERIALIZED(SELECT 1/0 AS n) SELECT 1 FROM c",""},
        {"SELECT 1 FROM(SELECT 1/0 AS n LIMIT 0)q",""},
        {"SELECT 1 FROM(SELECT DISTINCT 1/0 AS n)q","22012"},
        {"SELECT 1 FROM(SELECT plan_constant_writer(1/0) AS n)q","22012"},
        {"SELECT 1 FROM(SELECT CASE WHEN false THEN plan_constant_writer(1/0) ELSE 2 END AS n)q",""},
        {"SELECT 1 FROM(SELECT(SELECT 1/0) AS n)q",""},
        {"SELECT 1 FROM(SELECT abs(1/0) AS n)q",""},
        {"SELECT 1=ANY(SELECT 1/0)","22012"},
        {"SELECT CASE WHEN false THEN 1=ANY(SELECT 1/0) ELSE true END",""},
        {"SELECT 1=ANY(ARRAY[1,1/0])","22012"},
        {"SELECT unnest(ARRAY[1/0]) LIMIT 0","22012"},
        {"SELECT count(1/0) FILTER(WHERE false) FROM plan_constant_rows","22012"},
        {"SELECT COALESCE(1,1/0)",""},
        {"SELECT COALESCE(NULL,1,1/0)",""},
        {"SELECT COALESCE(id,1/0) FROM plan_constant_rows","22012"},
        {"SELECT COALESCE(true,false) OR 1/0=0",""},
        {"SELECT CASE WHEN true THEN COALESCE(1,1/0) ELSE 2 END",""},
        {"SELECT COALESCE(1,(SELECT 1/0))",""},
        {"SELECT(SELECT q.n) FROM(SELECT 1/0 AS n)q","22012"},
        {"WITH c AS(SELECT 1/0 AS n) SELECT(SELECT c.n) FROM c","22012"},
        {"SELECT CASE WHEN false THEN(SELECT q.n) ELSE 1 END FROM(SELECT 1/0 AS n)q",""},
        {"SELECT 1 FROM(SELECT 1/0 AS n)a JOIN(SELECT 1 AS n)b USING(n)","22012"},
        {"SELECT 1 FROM(SELECT 1/0 AS n)a NATURAL JOIN(SELECT 1 AS n)b","22012"},
        {"SELECT 1 FROM(SELECT 1/0 AS n ORDER BY n)q","22012"},
        {"SELECT 1 FROM(SELECT 1/0 AS n ORDER BY 1)q","22012"},
        {"WITH c AS(SELECT 1/0 AS n) SELECT 1 FROM c a,c b","22012"},
        {"WITH c AS NOT MATERIALIZED(SELECT 1/0 AS bad,1 AS good) SELECT a.good,b.bad FROM c a,c b","22012"},
        {"SELECT good FROM(SELECT * FROM(SELECT 1/0 AS bad,1 AS good)t)q",""},
        {"SELECT bad FROM(SELECT * FROM(SELECT 1/0 AS bad,1 AS good)t)q","22012"},
        {"SELECT \"good\" FROM(SELECT d.* FROM(SELECT 1/0 AS \"bad.name\",1 AS \"good\")d)q",""},
        {"WITH c AS(SELECT CASE WHEN false THEN plan_constant_writer(1) ELSE 1 END AS v,1/0 AS n) SELECT v FROM c","22012"},
    };
    size_t childOpens = 0, failures = 0;
    for (const auto& control : cases) {
        std::string actual;
        try {
            auto prepared = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,control.first));
            PreparedQueryExecution execution(prepared,&engine,db);
            execution.setQueryExecutor([&](const Stmt*,const RowContext&,size_t) -> PreparedQueryRows {
                ++childOpens; throw DbError("XX000","pure planning opened a child query");
            });
            execution.setChildCursorFactory([&](const Stmt*,const RowContext&) -> std::unique_ptr<PreparedQueryCursor> {
                ++childOpens; throw DbError("XX000","pure planning constructed a child cursor");
            });
            execution.planStatementConstants(prepared->ast.get());
        } catch (const DbError& error) { actual = error.sqlState(); }
        if (actual != control.second) ++failures;
        std::cout << "PURE PLAN " << control.first << " actual=" << actual
                  << " expected=" << control.second << std::endl;
    }
    assert(childOpens == 0);
    assert(engine.nextval(db,"plan_constant_effects") == 1); // no planning effects
    assert(failures == 0);

    // Planning cannot obtain a constant from a typed parameter's current
    // value, nor mutate the shared original AST/its original child sites.
    auto query = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT $1::INT,CASE WHEN false THEN(SELECT 1/0) ELSE 2 END",
        {{"p","p","text",{},true,"bad",1}}));
    auto* select = static_cast<SelectStmt*>(query->ast.get());
    auto* originalCase = dynamic_cast<CaseExpr*>(select->selectList[1].expr.get());
    assert(originalCase && originalCase->whenClauses.size()==1);
    const auto* child = originalCase->whenClauses[0].second->preparedSubquery.get();
    PreparedQueryExecution runtime(query,&engine,db);
    runtime.planStatementConstants(query->ast.get());
    assert(originalCase->whenClauses.size()==1 && originalCase->whenClauses[0].second->preparedSubquery.get()==child);
    runtime.prepareExpression(select->selectList[0].expr.get());
    bool rejected = false;
    try { (void)runtime.evaluate(select->selectList[0].expr.get(),runtime.context()); }
    catch (const DbError& error) { rejected=error.sqlState()=="22P02"; }
    assert(rejected); // conversion remains a runtime error for the actual cell
    runtime.prepareExpression(select->selectList[1].expr.get());
    assert(runtime.evaluate(select->selectList[1].expr.get(),runtime.context()).value=="2");
    LiteralExpr foreign; foreign.value="1/0";
    rejected=false;
    try { runtime.planExpressionConstants(&foreign); }
    catch(const DbError& error) { rejected=error.sqlState()=="XX000"; }
    assert(rejected);
    SelectStmt foreignStatement;
    rejected=false;
    try { runtime.planStatementConstants(&foreignStatement); }
    catch(const DbError& error) { rejected=error.sqlState()=="XX000"; }
    assert(rejected);
    auto normal = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT 1/0"));
    auto* normalExpr = static_cast<SelectStmt*>(normal->ast.get())->selectList.front().expr.get();
    PreparedQueryExecution defaultTiming(normal,&engine,db);
    defaultTiming.prepareExpression(normalExpr); // default timing stays lazy
    rejected=false;
    try { (void)defaultTiming.evaluate(normalExpr,defaultTiming.context()); }
    catch(const DbError& error) { rejected=error.sqlState()=="22012"; }
    assert(rejected);

    auto typed = std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT ARRAY[1,2]||ARRAY[3],1=ANY(ARRAY[1,2]),unnest(ARRAY[1,2]),COALESCE(NULL::BIGINT,2)"));
    auto* typedSelect=static_cast<SelectStmt*>(typed->ast.get());
    const auto* originalQuant=dynamic_cast<QuantifiedComparisonExpr*>(typedSelect->selectList[1].expr.get());
    const auto* originalSrf=dynamic_cast<FunctionCallExpr*>(typedSelect->selectList[2].expr.get());
    assert(originalQuant && originalQuant->comparison && originalSrf && originalSrf->setReturning);
    const auto identity=originalQuant->comparison->identity;
    PreparedQueryExecution typedRuntime(typed,&engine,db);
    typedRuntime.planStatementConstants(typed->ast.get());
    assert(originalQuant->comparison->identity==identity && originalSrf->setReturning);
    for(size_t i:{size_t(0),size_t(1),size_t(3)})typedRuntime.prepareExpression(typedSelect->selectList[i].expr.get());
    const auto joined=typedRuntime.evaluate(typedSelect->selectList[0].expr.get(),typedRuntime.context());
    assert(joined.typeName=="integer[]" && joined.value=="{1,2,3}");
    assert(typedRuntime.evaluate(typedSelect->selectList[1].expr.get(),typedRuntime.context()).asBool());
    const auto wide=typedRuntime.evaluate(typedSelect->selectList[3].expr.get(),typedRuntime.context());
    assert(wide.typeName=="bigint" && wide.value=="2");

    auto star=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT d.* FROM(SELECT 1 AS \"K\",2 AS \"K\")d"));
    auto* starSelect=static_cast<SelectStmt*>(star->ast.get());
    auto& outputs=star->projectionBindings.at(starSelect);
    assert(outputs.size()==2 && outputs[0].expression==outputs[1].expression);
    assert(outputs[0].column && outputs[1].column &&
        outputs[0].column->sourceOrdinal==outputs[1].column->sourceOrdinal &&
        outputs[0].column->columnOrdinal==0 && outputs[1].column->columnOrdinal==1);
    PreparedQueryExecution starRuntime(star,&engine,db);
    starRuntime.planStatementConstants(star->ast.get());
    outputs[1].column->columnOrdinal=99;
    rejected=false;
    try { PreparedQueryExecution invalidProjection(star,&engine,db); }
    catch(const DbError& error) { rejected=error.sqlState()=="XX000"; }
    assert(rejected); // no names/value-discovery escape for star metadata
    std::cout << "[PREPARED CONSTANT PLANNING] " << cases.size()
              << " PG18 controls, pure owner/parameter/original-AST guards passed\n";
    cleanupTestDb(name);
}
