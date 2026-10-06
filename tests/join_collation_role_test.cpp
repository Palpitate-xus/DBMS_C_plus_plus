#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name="join_collation_role", db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db,"utf8")==dbms::DBStatus::OK);
    Session session;
    session.username="admin";
    session.permission=1;
    session.currentDB=db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE jl(id INT,t TEXT,\"C\" TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE jr(id INT,t TEXT,\"C\" TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE je(id INT,t TEXT)",session));
    for (const std::string table : {"jl","jr"}) {
        assert(g_engine.insert(db,table,{{"id","1"},{"t","a"},{"C","datum"}})==dbms::DBStatus::OK);
        assert(g_engine.insert(db,table,{{"id","2"},{"t","NULL"},{"C","empty"}})==dbms::DBStatus::OK);
    }
    const dbms::StorageEngine::JoinRangeNames ranges{"l","r",true};
    const std::set<std::string> columns{"l._6964","r._6964"};
    assert(g_engine.join(db,"jl","jr","","",{},columns,nullptr,nullptr,
        {"typedexpr (l.t COLLATE \"C\") IS NOT DISTINCT FROM (r.t COLLATE \"C\")"},ranges)==
        (std::vector<std::string>{"1 1 ","2 2 "}));
    for (const std::string table : {"jl","je"}) {
        bool rejected=false;
        try { g_engine.join(db,table,"jr","","",{},columns,nullptr,nullptr,
            {"typedexpr (l.t COLLATE \"missing_join_collation\") IS NOT DISTINCT FROM r.t"},ranges); }
        catch(const dbms::DbError& e) { rejected=e.sqlState()=="42704"; }
        assert(rejected);
    }
    assert(g_engine.insert(db,"je",{{"id","3"},{"t","a"}})==dbms::DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"[JOIN COLLATION ROLE] label preparation and nullable values passed\n";
}
