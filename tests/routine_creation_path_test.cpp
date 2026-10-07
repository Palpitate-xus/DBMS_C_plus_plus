#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="routine_creation_path", db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="Routine.Creator"; session.permission=1; session.currentDB=db;
    auto* previous=currentSession(); setCurrentSession(&session);
    struct Restore {Session* session; ~Restore(){setCurrentSession(session);}} restore{previous};
    DdlExecutor ddl; int failed=0;
    const auto check=[&](bool value,const std::string& label) {
        std::cout<<"[ROUTINE CREATION PATH] "<<label<<" "<<(value?"PASS":"FAIL")<<"\n";
        if(!value)++failed;
    };
    const auto create=[&](const std::string& name,int value,bool replace=false) {
        return !ddl.executeSql("CREATE "+std::string(replace?"OR REPLACE ":"")+"FUNCTION "+name+
            "() RETURNS INT LANGUAGE SQL AS $$SELECT "+std::to_string(value)+"$$",session);
    };
    const auto body=[&](const std::string& function,const std::string& schema,int value) {
        const auto stored=g_engine.getUDF(db,function,schema);
        return !stored.expression.empty() && stored.expression.find(std::to_string(value))!=std::string::npos;
    };
    assert(!ddl.executeSql("CREATE SCHEMA routine_create_a",session));
    assert(!ddl.executeSql("CREATE SCHEMA \"Routine.Create.B\"",session));
    assert(!ddl.executeSql("CREATE SCHEMA \"Routine.Creator\"",session));
    session.searchPath="routine_create_missing, \"Routine.Create.B\", routine_create_a, public";
    check(create("creation_chosen",11),"first existing quoted namespace creation");
    check(body("creation_chosen","Routine.Create.B",11),"actual nonpublic bytes");
    check(!g_engine.udfExists(db,"creation_chosen"),"not incorrectly public");
    session.searchPath="routine_create_a, \"Routine.Create.B\", public";
    check(create("creation_chosen",22),"same name distinct creation namespace");
    check(body("creation_chosen","routine_create_a",22),"first namespace retained");
    session.searchPath="routine_create_missing, \"Routine.Create.B\", routine_create_a";
    check(create("creation_chosen",33,true),"replacement chooses creation namespace");
    check(body("creation_chosen","Routine.Create.B",33),"actual selected replacement");
    check(body("creation_chosen","routine_create_a",22),"other occurrence unchanged");
    session.searchPath="\"$user\", routine_create_a, public";
    check(create("creation_user",88),"username-expanded creation");
    check(body("creation_user","Routine.Creator",88),"actual username namespace");
    session.searchPath="routine_create_a";
    check(create("public.creation_explicit",77),"explicit namespace overrides creation path");
    check(body("creation_explicit","public",77),"explicit public bytes");
    for(const auto& path:{std::string{},std::string{"routine_create_missing"}}) {
        session.searchPath=path; std::string state;
        try {(void)create("creation_rejected",55);}
        catch(const DbError& error){state=error.sqlState();}
        check(state=="3F000","no actual creation namespace");
        check(!g_engine.udfExists(db,"creation_rejected"),"no missing-path public artifact");
    }
    session.searchPath="routine_create_a";
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    check(create("creation_undone",66),"transaction creation");
    check(body("creation_undone","routine_create_a",66),"transaction actual namespace");
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    check(!g_engine.udfExists(db,"creation_undone","routine_create_a"),"undo selected occurrence");
    check(body("creation_chosen","routine_create_a",22),"undo preserves preexisting namespace");
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[ROUTINE CREATION PATH] failed="<<failed<<"\n";
    return failed?1:0;
}
