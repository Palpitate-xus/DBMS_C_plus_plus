#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "process/OutputCapture.h"
#include "test_utils.h"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="readonly_temp_target",db=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(db)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    for(const auto* sql:{"CREATE TABLE r314_shadow(id integer,n integer)","CREATE TABLE r314_source(id integer,n integer)",
            "CREATE TEMP TABLE r314_temp(id integer,n integer)","CREATE TEMP TABLE r314_shadow(id integer,n integer)",
            "CREATE TEMP TABLE \"R314.Dot\"(id integer,n integer)"})
        if(ddl.executeSql(sql,session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label){++controls;if(!good){++failures;std::cerr<<"READONLY_TEMP_NATIVE_FAIL "<<label<<'\n';}};
    const auto bridge=[&](const std::string& sql) {
        SQLParser parser;const auto parsed=parser.parse(sql);if(!parsed.success)return std::string("parse failure");
        bool handled=false,error=true;std::string state;std::ostringstream output;
        try {ScopedOutputCapture capture(output);error=tryDmlBridge(sql,parsed.stmt->command,session,handled,sql);}
        catch(const DbError& e){state=e.sqlState();}
        catch(const std::exception& e){state=e.what();}
        if(state.empty() && error) {
            const auto at=output.str().find("SQLSTATE ");
            state=at==std::string::npos?output.str():output.str().substr(at+9,5);
        }
        if(state.empty() && !handled)return std::string("not handled");
        return state;
    };
    for(const auto* sql:{"INSERT INTO public.r314_shadow VALUES(1,100)","INSERT INTO r314_source VALUES(1,9)",
            "INSERT INTO r314_temp VALUES(1,1)","INSERT INTO pg_temp.r314_shadow VALUES(1,1)",
            "INSERT INTO \"pg_temp\".\"R314.Dot\" VALUES(1,1)"})if(!bridge(sql).empty())return 2;
    const auto rows=[&](const std::string& physical) {
        std::vector<std::vector<std::string>> values;std::vector<std::vector<bool>> nulls;
        (void)g_engine.query(db,physical,{}, {"id","n"},{},false,false,false,0,{},&values,&nulls);
        return values;
    };
    const std::vector<std::string> targets={"r314_temp","pg_temp.r314_temp","\"pg_temp\".\"r314_temp\"",
        "\"pg_temp\".\"R314.Dot\"","r314_shadow","public.r314_shadow"};
    for(const auto& target:targets)for(const auto* operation:{"INSERT","UPDATE","DELETE","MERGE"}) {
        const bool durable=target=="public.r314_shadow";
        const std::string name=target.find("R314.Dot")!=std::string::npos?"R314.Dot":target.find("shadow")!=std::string::npos?"r314_shadow":"r314_temp";
        const std::string physical=durable?"r314_shadow":tempTablePrefix(session,name);
        const auto before=rows(physical);
        std::string sql;
        if(std::string(operation)=="INSERT")sql="INSERT INTO "+target+" VALUES(2,2)";
        if(std::string(operation)=="UPDATE")sql="UPDATE "+target+" SET n=n+1 WHERE id=1";
        if(std::string(operation)=="DELETE")sql="DELETE FROM "+target+" WHERE id=1";
        if(std::string(operation)=="MERGE")sql="MERGE INTO "+target+" AS t USING r314_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.n";
        if(g_engine.beginTransaction(db)!=DBStatus::OK || !g_engine.setReadOnly(true))return 2;
        const auto state=bridge(sql);
        check(state==(durable?"25006":""),sql+" state="+state);
        check(g_engine.inTransaction() && g_engine.isReadOnly(),sql+" mode retained");
        auto expected=before;
        if(!durable) {
            if(std::string(operation)=="INSERT")expected.push_back({"2","2"});
            if(std::string(operation)=="UPDATE")expected={{"1","2"}};
            if(std::string(operation)=="DELETE")expected.clear();
            if(std::string(operation)=="MERGE")expected={{"1","9"}};
        }
        check(rows(physical)==expected,sql+" actual target values");
        check(g_engine.rollbackTransaction()==DBStatus::OK && !g_engine.isReadOnly() && rows(physical)==before,sql+" rollback and mode");
    }
    for(const std::string path:std::vector<std::string>{"public","public, pg_temp","pg_temp, public","public, "+sessionTempSchemaName(session)}) {
        session.searchPath=path;
        const bool explicitLate=path.rfind("public,",0)==0;
        const auto query=g_engine.prepareBoundQuery(db,"SELECT id,n FROM r314_shadow");
        const auto range=query.sourceRanges.front();
        check(range.relationSchema==(explicitLate?"public":sessionTempSchemaName(session)),"metadata target "+path);
        for(const auto* operation:{"INSERT","UPDATE","DELETE"}) {
            const std::string sql=std::string(operation)=="INSERT"?"INSERT INTO r314_shadow VALUES(2,2)":std::string(operation)=="UPDATE"?"UPDATE r314_shadow SET n=9 WHERE id=1":"DELETE FROM r314_shadow WHERE id=1";
            if(g_engine.beginTransaction(db)!=DBStatus::OK || !g_engine.setReadOnly(true))return 2;
            const auto state=bridge(sql);
            check(state==(explicitLate?"25006":""),sql+" path="+path+" state="+state);
            check(g_engine.rollbackTransaction()==DBStatus::OK,"path rollback");
        }
    }
    cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"READONLY_TEMP_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
