#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="insert_source_preflight", db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    for(const auto& tableName:{"target_rows","source_rows"}) {
        TableSchema table; table.append(makeIntColumn("id",false,2));
        table.append(makeIntColumn("v",true,2));
        assert(g_engine.createTable(db,tableName,table)==DBStatus::OK);
    }
    assert(g_engine.insertRow(db,"source_rows",{{"id","1"},{"v","10"}})==DBStatus::OK);
    assert(g_engine.createSequence(db,"source_effects",1,1)==DBStatus::OK);
    Session session; session.username="source_reader"; session.permission=0; session.currentDB=db;
    g_engine.grant(db,"target_rows",session.username,StorageEngine::TablePrivilege::Insert);
    size_t checks=0,failures=0;
    const auto check=[&](bool good,const char* label) {
        ++checks;if(!good){++failures;std::cerr<<"INSERT_SOURCE_PREFLIGHT_FAILURE "<<label<<'\n';}
    };
    const auto run=[&](const std::string& source,bool allowed) {
        clearLastDmlResult();bool handled=false;std::string state;
        const auto sql="INSERT INTO target_rows "+source;
        try { if(tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql))state="unstructured-error";
              else if(!handled)state="legacy-fallback"; }
        catch(const DbError& error){state=error.sqlState();}
        const auto result=takeLastDmlResult();
        std::cout<<"INSERT_SOURCE_PREFLIGHT "<<sql<<" STATE "<<state<<std::endl;
        check(state==(allowed?"":"42501"),"source metadata preflight has the expected result");
        check(!g_engine.inTransaction(),"statement owner is released");
        if(!allowed) {
            check(!result.available,"denied source publishes no rows");
            check(g_engine.query(db,"target_rows",{}, {"id"}).empty(),"denied source cannot write a target prefix");
        }
        assert(g_engine.remove(db,"target_rows",{})==DBStatus::OK);
    };
    for(const auto& source:{
        "SELECT id,v FROM source_rows", "SELECT id,v FROM source_rows WHERE FALSE",
        "SELECT 2,(SELECT v FROM source_rows)",
        "SELECT s.id,s.v FROM (SELECT id,v FROM source_rows) s"}) run(source,false);
    for(const auto& source:{"SELECT id,v FROM source_rows", "SELECT id,v FROM source_rows WHERE FALSE",
                           "SELECT 2,(SELECT v FROM source_rows)"}) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"INSERT INTO target_rows "+std::string(source)));
        std::string state;
        try { prepareBoundDml(query->ast.get(),session,query); }
        catch(const DbError& error){state=error.sqlState();}
        check(state=="42501","prepared mutation constructor uses the same pure source preflight");
    }
    g_engine.grant(db,"source_rows",session.username,StorageEngine::TablePrivilege::Select,{"id"});
    run("SELECT id,10 FROM source_rows",true);
    run("SELECT id,v FROM source_rows",false);
    run("SELECT id,10 FROM source_rows WHERE v=10",false);
    run("SELECT id,10 FROM source_rows ORDER BY v",false);
    run("SELECT * FROM source_rows",false);
    run("SELECT s.id,p.id FROM source_rows s JOIN source_rows p USING(v)",false);
    run("SELECT s.id,p.id FROM source_rows s NATURAL JOIN source_rows p",false);
    g_engine.grant(db,"source_rows",session.username,StorageEngine::TablePrivilege::Select);
    run("SELECT id,v FROM source_rows",true);
    run("SELECT id,v FROM source_rows WHERE FALSE",true);
    run("SELECT 2,(SELECT v FROM source_rows)",true);
    run("SELECT s.id,s.v FROM source_rows s JOIN source_rows p ON s.id=p.id",true);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"INSERT_SOURCE_PREFLIGHT_CHECKS "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
