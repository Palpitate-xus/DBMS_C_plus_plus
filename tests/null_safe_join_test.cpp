#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "null_safe_join";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE jl(id INT,k INT,v TEXT)", session));
    assert(!ddl.executeSql("CREATE TABLE jr(id INT,k INT,v TEXT)", session));
    for (const auto& row : std::vector<std::vector<std::string>>{{"1","NULL","NULL"},{"2","2",""},{"3","3","x"},{"4","4","left"}})
        assert(g_engine.insert(database,"jl",{{"id",row[0]},{"k",row[1]},{"v",row[2]}})==dbms::DBStatus::OK);
    for (const auto& row : std::vector<std::vector<std::string>>{{"10","NULL","NULL"},{"20","2",""},{"30","3","x"},{"50","5","right"}})
        assert(g_engine.insert(database,"jr",{{"id",row[0]},{"k",row[1]},{"v",row[2]}})==dbms::DBStatus::OK);
    const dbms::StorageEngine::JoinRangeNames ranges{"l","r",true};
    const auto key = [](const std::string& range,const std::string& column) { return dbms::StorageEngine::joinRangeColumnKey(range,column,true); };
    const std::set<std::string> projection{key("l","id"),key("r","id")};
    const std::vector<std::string> same{"typedexpr l.k IS NOT DISTINCT FROM r.k"};
    const std::vector<std::string> matching{"1 10 ","2 20 ","3 30 "};
    assert(g_engine.join(database,"jl","jr","k","k",{},projection,nullptr,nullptr,{},ranges)==(std::vector<std::string>{"2 20 ","3 30 "}));
    assert(g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,same,ranges)==matching);
    assert(g_engine.leftJoin(database,"jl","jr","","",{},projection,nullptr,nullptr,same,ranges)==(std::vector<std::string>{"1 10 ","2 20 ","3 30 ","4 NULL "}));
    assert(g_engine.rightJoin(database,"jl","jr","","",{},projection,nullptr,nullptr,same,ranges)==(std::vector<std::string>{"1 10 ","2 20 ","3 30 ","NULL 50 "}));
    for (const auto& predicate : {"CAST(l.k AS TEXT) IS NOT DISTINCT FROM CAST(r.k AS TEXT)","l.k IS NOT DISTINCT FROM abs(-r.k)","l.k IS NOT DISTINCT FROM CASE WHEN r.id=20 THEN 2 ELSE r.k END","(l.k+1) IS NOT DISTINCT FROM (r.k+1)","l.v IS NOT DISTINCT FROM r.v"})
        assert(g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,{"typedexpr "+std::string(predicate)},ranges)==matching);
    assert(g_engine.createUDF(database, "join_identity", {"k"}, {"int"}, "BEGIN RETURN k; END;", 's', "plpgsql", "int") == dbms::DBStatus::OK);
    assert(g_engine.createUDF(database, "join_strict", {"k"}, {"int"}, "BEGIN RETURN k; END;", 's', "plpgsql", "int", true) == dbms::DBStatus::OK);
    assert(g_engine.createUDF(database, "JoinIdentity", {"k"}, {"int"}, "BEGIN RETURN k; END;", 'i', "plpgsql", "int") == dbms::DBStatus::OK);
    for (const auto& function : {"join_identity", "join_strict", "public.\"JoinIdentity\""})
        assert(g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,{"typedexpr l.k IS NOT DISTINCT FROM "+std::string(function)+"(r.k)"},ranges)==matching);
    assert(!ddl.executeSql("CREATE TABLE join_prepare_sink(id INT)", session));
    assert(g_engine.createUDF(database, "join_prepare_writer", {"k"}, {"int"}, "BEGIN INSERT INTO join_prepare_sink VALUES(k); RETURN k; END;", 'v', "plpgsql", "int") == dbms::DBStatus::OK);
    bool invalidCall = false;
    try { g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,{"typedexpr join_prepare_writer(l.k) IS NOT DISTINCT FROM no_such_join_function(r.k)"},ranges); }
    catch (const dbms::DbError& e) { invalidCall = e.sqlState() == "42883"; }
    assert(invalidCall && g_engine.getTableRowCount(database, "join_prepare_sink") == 0);
    assert(!ddl.executeSql("CREATE TABLE full_once_l(id INT)", session));
    assert(!ddl.executeSql("CREATE TABLE full_once_r(id INT)", session));
    assert(g_engine.insert(database,"full_once_l",{{"id","1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database,"full_once_r",{{"id","1"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE SEQUENCE full_once_calls", session));
    assert(g_engine.createUDF(database, "full_once_identity", {"k"}, {"int"}, "BEGIN PERFORM nextval('full_once_calls'); RETURN k; END;", 'v', "plpgsql", "int") == dbms::DBStatus::OK);
    assert(g_engine.fullOuterJoin(database,"full_once_l","full_once_r","id","id",{}, {},nullptr,nullptr,{"typedexpr l.id IS NOT DISTINCT FROM full_once_identity(r.id)"},ranges) == (std::vector<std::string>{"1 1 "}));
    assert(g_engine.currval(database, "full_once_calls") == 1);
    assert(g_engine.insert(database,"full_once_r",{{"id","1"}}) == dbms::DBStatus::OK);
    assert(g_engine.fullOuterJoin(database,"full_once_l","full_once_r","id","id",{}, {},nullptr,nullptr,{"typedexpr l.id IS NOT DISTINCT FROM full_once_identity(r.id)"},ranges) == (std::vector<std::string>{"1 1 ","1 1 "}));
    assert(g_engine.currval(database, "full_once_calls") == 3);
    assert(g_engine.fullOuterJoin(database,"full_once_l","full_once_r","id","id",{"=l._6964 99"}, {},nullptr,nullptr,{"typedexpr l.id IS NOT DISTINCT FROM r.id"},ranges).empty());
    assert(g_engine.insert(database,"full_once_l",{{"id","NULL"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database,"full_once_r",{{"id","NULL"}}) == dbms::DBStatus::OK);
    assert(g_engine.fullOuterJoin(database,"full_once_l","full_once_r","id","id",{}, {},nullptr,nullptr,{"typedexpr l.id IS NOT DISTINCT FROM r.id"},ranges) == (std::vector<std::string>{"1 1 ","1 1 ","NULL NULL ","NULL NULL "}));
    assert(g_engine.leftJoin(database,"jl","jr","","",{},projection,nullptr,nullptr,{same[0],"=r._6964 20"},ranges)==(std::vector<std::string>{"1 NULL ","2 20 ","3 NULL ","4 NULL "}));
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    g_engine.leftJoin(database,"jl","jr","","",{},projection,&cells,&nulls,same,ranges);
    assert(cells.size()==4 && nulls.size()==4 && cells[3][0]=="4" && nulls[3]==(std::vector<bool>{false,true}));
    bool rejected=false;
    try { g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,{"typedexpr l.k IS NOT DISTINCT FROM CAST('bad' AS INT)"},ranges); }
    catch (const dbms::DbError& e) { rejected=e.sqlState()=="22P02"; }
    assert(rejected);
    assert(g_engine.insert(database,"jl",{{"id","6"},{"k","6"}})==dbms::DBStatus::OK);
    assert(g_engine.join(database,"jl","jr","","",{},projection,nullptr,nullptr,same,ranges)==matching);
    cleanupTestDb(testName);
    dbms::setCurrentSession(nullptr);
    std::cout << "[NULL SAFE JOIN] typed ON, NULL bitmap and outer-row preservation passed\n";
}
