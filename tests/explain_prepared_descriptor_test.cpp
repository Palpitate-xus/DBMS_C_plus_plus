#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("explain_prepared_descriptor");assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE SEQUENCE descriptor_calls",session));
    assert(!ddl.executeSql("CREATE TABLE descriptor_rows(v INT DEFAULT nextval('descriptor_calls'))",session));
    for(const std::string command:{"INSERT INTO descriptor_rows DEFAULT VALUES RETURNING v",
        "UPDATE descriptor_rows SET v=nextval('descriptor_calls') WHERE false RETURNING v",
        "DELETE FROM descriptor_rows WHERE false RETURNING v"}) {
        for(bool json:{true,false}) {
            auto prepared=g_engine.prepareBoundQuery(db,"EXPLAIN (ANALYZE TRUE,FORMAT "+
                std::string(json?"JSON":"TEXT")+") "+command);
            assert(prepared.output.size()==1 && prepared.output[0].name=="QUERY PLAN");
            assert(prepared.output[0].type==(json?"json":"text") && prepared.output[0].typeOid==(json?114u:25u));
            const auto* explain=dynamic_cast<const ExplainStmt*>(prepared.ast.get());assert(explain && explain->query);
            const auto& descriptor=prepared.statementOutputs.at(prepared.ast.get());
            assert(descriptor.size()==1 && descriptor[0].name==prepared.output[0].name &&
                descriptor[0].type==prepared.output[0].type && descriptor[0].typeOid==prepared.output[0].typeOid);
        }
    }
    QueryBindingDatum parameter;parameter.identity="descriptor-position-1";parameter.type="bigint";parameter.position=1;
    auto typed=g_engine.prepareBoundQuery(db,"EXPLAIN (FORMAT JSON) INSERT INTO descriptor_rows(v) VALUES($1) RETURNING v",{parameter});
    assert(typed.output[0].type=="json" && typed.parameters.size()==1 && typed.parameters[0].typeName=="bigint" && typed.parameters[0].isNull);
    const auto* explain=dynamic_cast<const ExplainStmt*>(typed.ast.get());
    const auto* insert=explain?dynamic_cast<const InsertStmt*>(explain->query.get()):nullptr;assert(insert);
    const auto* value=dynamic_cast<const ParameterExpr*>(insert->values[0][0].get());
    assert(value && value->slot==0 && value->declaredType=="bigint" && insert->preparedDefaults.empty());
    bool noCalls=false;try{(void)g_engine.currval(db,"descriptor_calls");}
    catch(const DbError& error){noCalls=error.sqlState()=="55000";}assert(noCalls);
    auto arithmetic=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "EXPLAIN UPDATE descriptor_rows SET v=1/0 WHERE false"));
    assert(arithmetic->output[0].type=="text");
    PreparedQueryExecution planning(arithmetic,&g_engine,db);bool division=false;
    try{planning.planStatementConstants(arithmetic->ast.get());}
    catch(const DbError& error){division=error.sqlState()=="22012";}assert(division);
    bool input=false;try{(void)g_engine.prepareBoundQuery(db,
        "EXPLAIN UPDATE descriptor_rows SET v=CAST('bad' AS INT) WHERE false");}
    catch(const DbError& error){input=error.sqlState()=="22P02";}assert(input);
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb("explain_prepared_descriptor");setCurrentSession(nullptr);
    std::cout<<"[EXPLAIN PREPARED DESCRIPTOR] genuine whole types/typed slots/phase/noeffects passed\n";
}
