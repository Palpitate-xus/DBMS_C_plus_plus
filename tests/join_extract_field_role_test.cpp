#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name="join_extract_field_role",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db,"utf8")==dbms::DBStatus::OK);
    Session session;
    session.username="admin";
    session.permission=1;
    session.currentDB=db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE jl(id INT,day DATE,year INT)",session));
    assert(!ddl.executeSql("CREATE TABLE jr(id INT,day DATE,year INT)",session));
    assert(!ddl.executeSql("CREATE TABLE je(id INT,day DATE)",session));
    for(const std::string table:{"jl","jr"}) {
        assert(g_engine.insert(db,table,{{"id","1"},{"day","2026-10-06"},{"year","9999"}})==dbms::DBStatus::OK);
        assert(g_engine.insert(db,table,{{"id","2"},{"day","NULL"},{"year","8888"}})==dbms::DBStatus::OK);
    }
    const dbms::StorageEngine::JoinRangeNames ranges{"l","r",true};
    const std::set<std::string> projection{"l._6964","r._6964"};
    const std::vector<std::string> expected{"1 1 ","2 2 "};
    assert(g_engine.join(db,"jl","jr","","",{},projection,nullptr,nullptr,
        {"typedexpr date_part('year',l.day) IS NOT DISTINCT FROM date_part('year',r.day)"},ranges)==expected);
    for(const std::string field:{"year","\"YEAR\""})
        assert(g_engine.join(db,"jl","jr","","",{},projection,nullptr,nullptr,
            {"typedexpr EXTRACT("+field+" FROM l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day)"},ranges)==expected);
    assert(g_engine.join(db,"je","jr","","",{},projection,nullptr,nullptr,
        {"typedexpr EXTRACT(year FROM l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day)"},ranges).empty());
    cleanupTestDb(name);
    std::cout<<"[JOIN EXTRACT FIELD ROLE] field labels and data references passed\n";
}
