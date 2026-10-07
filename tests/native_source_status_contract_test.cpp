#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const std::string name="native_source_status_contract",database=testDbPath(name);cleanupTestDb(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE atomic_target(id INT PRIMARY KEY,val INT UNIQUE)",session));
    assert(!ddl.executeSql("CREATE TABLE atomic_source(id INT,val INT)",session));
    for(const auto& pair:std::vector<std::pair<std::string,std::string>>{{"1","10"},{"2","20"},{"3","40"}})
        assert(g_engine.insertRow(database,"atomic_target",{{"id",pair.first},{"val",pair.second}})==DBStatus::OK);
    for(const auto& pair:std::vector<std::pair<std::string,std::string>>{{"1","30"},{"2","40"}})
        assert(g_engine.insertRow(database,"atomic_source",{{"id",pair.first},{"val",pair.second}})==DBStatus::OK);
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&]{Snapshot value;(void)g_engine.query(database,"atomic_target",{},{"id","val"},{{"id",true}},false,false,false,0,{},&value.first,&value.second);return value;};
    const std::string sql="UPDATE atomic_target AS dst SET val = src.val FROM atomic_source AS src WHERE dst.id = src.id";
    size_t failures=0;
    for(const bool parent:{false,true}) {
        if(parent){assert(g_engine.beginTransaction(database)==DBStatus::OK);assert(g_engine.insertRow(database,"atomic_target",{{"id","9"},{"val","90"}})==DBStatus::OK);}
        const auto prior=snapshot();bool handled=false,error=false;std::string state;
        try{error=tryDmlBridge(sql,SqlCommand::Update,session,handled,sql);}
        catch(const DbError& failure){state=failure.sqlState();}
        std::cout<<"SOURCE_STATUS_API PARENT "<<parent<<" ERROR "<<error<<" HANDLED "<<handled<<" EXCEPTION "<<state<<std::endl;
        if(!error || !handled || !state.empty() || snapshot()!=prior || g_engine.inTransaction()!=parent)++failures;
        if(parent)assert(g_engine.rollbackTransaction()==DBStatus::OK);
    }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"SOURCE_STATUS_API_FAILURES="<<failures<<std::endl;assert(failures==0);
}
