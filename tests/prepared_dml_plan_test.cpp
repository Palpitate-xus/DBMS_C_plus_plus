#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("prepared_dml_plan");assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE plan_calls",session));
    assert(g_engine.createUDF(db,"plan_default",{},{},"SELECT nextval('plan_calls')",'v',"sql","integer")==DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE plan_rows(id INT,v INT DEFAULT public.plan_default())",session));
    assert(!ddl.executeSql("CREATE TABLE plan_source(id INT,v INT)",session));
    assert(g_engine.insertRow(db,"plan_rows",{{"id","1"},{"v","10"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"plan_rows",{{"id","2"},{"v","20"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"plan_source",{{"id","1"},{"v","31"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"plan_source",{{"id","2"},{"v",std::nullopt}})==DBStatus::OK);
    const auto query=[&](const std::string& sql){return std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));};
    const auto noCalls=[&]{bool rejected=false;try{(void)g_engine.currval(db,"plan_calls");}catch(const DbError& error){rejected=error.sqlState()=="55000";}assert(rejected);};
    auto prepared=query("UPDATE plan_rows AS x SET v=DEFAULT WHERE false RETURNING x.v");
    auto plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{});
    assert(plan->preparedPlanNodeName()=="ModifyTable" && plan->preparedPlanAttributes().at("operation")=="Update");
    assert(plan->preparedPlanAttributes().at("relation")=="plan_rows" && plan->preparedPlanAttributes().at("alias")=="x");
    assert(plan->runtimeRows()==0 && plan->runtimeLoops()==0);noCalls();plan->close();noCalls();
    prepared=query("UPDATE plan_rows SET v=DEFAULT WHERE true RETURNING id,v");
    plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{});noCalls();
    assert(plan->open());std::string display;std::vector<ExprValue> row;
    assert(plan->next(display) && plan->lastStructuredValues(row) && row[1].value=="1");
    assert(plan->next(display) && plan->lastStructuredValues(row) && row[1].value=="2");
    assert(!plan->next(display) && plan->runtimeRows()==2 && g_engine.currval(db,"plan_calls")==2);
    plan->close();plan->close();bool once=false;try{(void)plan->open();}catch(const DbError& error){once=error.sqlState()=="XX000";}assert(once && g_engine.currval(db,"plan_calls")==2);
    prepared=query("UPDATE plan_rows AS x SET v=s.v FROM plan_source AS s WHERE x.id=s.id RETURNING x.v");
    size_t reads=0,scans=0;
    PreparedDmlSourceFactory factory=[&](const Stmt* owner,const FromItem* item,const RowContext& outer) {
        const auto range=std::find_if(prepared->sourceRanges.begin(),prepared->sourceRanges.end(),
            [&](const auto& range){return range.owner==owner && range.source==item;});assert(range!=prepared->sourceRanges.end());
        const auto ordinal=range->ordinal;const auto descriptor=range->columns;
        auto sourceQuery=query("SELECT id,v FROM plan_source");
        auto sourcePlan=std::make_shared<OpPtr>(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,
            sourceQuery,sourceQuery->ast.get()));
        auto rows=std::make_shared<std::vector<std::vector<ExprValue>>>();auto loaded=std::make_shared<bool>(false);
        return PreparedDmlSourceRows{{ordinal},[&,rows,loaded,sourcePlan,ordinal,descriptor,outer](size_t index,RowContext& context) {
            ++reads;
            if(!*loaded) {
                ++scans;*loaded=true;
                assert((*sourcePlan)->open());std::string display;
                while((*sourcePlan)->next(display)) {
                    std::vector<ExprValue> cells;assert((*sourcePlan)->lastStructuredValues(cells));
                    assert(cells.size()==descriptor.size());
                    rows->push_back(std::move(cells));
                }
                assert(!(*sourcePlan)->hasError());(*sourcePlan)->close();
            }
            if(index>=rows->size())return false;
            context=outer;for(size_t i=0;i<descriptor.size();++i)context.setBoundColumn(ordinal,i,rows->at(index)[i]);return true;
        }};
    };
    plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{},factory);
    assert(reads==0 && scans==0 && plan->preparedPlanChildren().size()==1);
    auto* source=plan->preparedPlanChildren().front();assert(source->preparedPlanNodeName()=="TypedSource");
    assert(plan->open());assert(plan->next(display) && plan->lastStructuredValues(row) && row[0].value=="31");
    assert(plan->next(display) && plan->lastStructuredValues(row) && row[0].isNull);
    assert(!plan->next(display) && source->runtimeRows()==2 && reads==3 && scans==1);plan->close();
    // A later tuple failure restores the earlier physical mutation. The
    // passed owner is scoped independently of the ambient native caller.
    prepared=query("UPDATE plan_rows SET v=1/(id-2) RETURNING id,v");
    Session ambient;ambient.currentDB="not_the_mutation_database";setCurrentSession(&ambient);
    plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{});
    assert(currentSession()==&ambient && plan->open());
    bool division=false;try{(void)plan->next(display);}catch(const DbError& error){division=error.sqlState()=="22012";}
    assert(division && currentSession()==&ambient && plan->runtimeRows()==0);plan->close();plan->close();
    setCurrentSession(&session);
    auto check=query("SELECT id,v FROM plan_rows ORDER BY id");
    const auto state=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,check,check->ast.get()));
    state.throwIfFailed();assert(state.structuredRows.size()==2 && state.structuredRows[0][1]=="31" && state.structuredNulls[1][1]);
    prepared=query("UPDATE plan_rows SET v=1/0 WHERE false");
    bool staticDivision=false;try{plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{});}catch(const DbError& error){staticDivision=error.sqlState()=="22012";}
    assert(staticDivision && g_engine.currval(db,"plan_calls")==2);
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);setCurrentSession(nullptr);cleanupTestDb("prepared_dml_plan");
    std::cout<<"[PREPARED DML PLAN] actual owned carrier/source/demand/NULL/counters passed\n";
}
