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
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="delete_bound_returning",database=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,v TEXT,c CHAR(3),b BYTEA,a INT[],\"V\" TEXT)",session));
    assert(g_engine.createIndex(database,"target","v")==DBStatus::OK);
    // The SQL regclass consumer needs the SQL catalog identity as well as a
    // native sequence file. The file-only API is not CREATE SEQUENCE DDL.
    assert(!ddl.executeSql("CREATE SEQUENCE effects",session));
    const std::vector<StorageEngine::SqlRow> originals={
        {{"id","1"},{"v","a"},{"c","a  "},{"b",std::string("\0A",2)},{"a","{1,2}"},{"V","Upper"}},
        {{"id","2"},{"v","ab"},{"c","é  "},{"b",std::string(1,static_cast<char>(0xff))},{"a",std::nullopt},{"V","NULL"}},
        {{"id","3"},{"v",""},{"c","   "},{"b",""},{"a","{}"},{"V",""}},
        {{"id","4"},{"v",std::nullopt},{"c",std::nullopt},{"b",std::nullopt},{"a",std::nullopt},{"V",std::nullopt}},
        {{"id","5"},{"v","NULL"},{"c","x  "},{"b",std::string(1,'\0')},{"a","[0:1]={3,4}"},{"V","tail"}},
    };
    const auto seed=[&]{for(const auto& row:originals)assert(g_engine.insertRow(database,"target",row)==DBStatus::OK);};
    seed();
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&] {
        Snapshot rows;
        (void)g_engine.query(database,"target",{},{"id","v","V"},{{"id",true}},false,false,false,0,{},&rows.first,&rows.second);
        return rows;
    };
    const auto original=snapshot();
    std::vector<std::string> failures;
    const auto check=[&](const std::string& label,bool good){if(!good){failures.push_back(label);std::cerr<<"DELETE_BOUND_NATIVE_FAILURE "<<label<<'\n';}};
    const auto run=[&](const std::string& sql,const std::string& expectedState={}) {
        bool handled=false,error=false;std::string state;
        clearLastDmlResult();
        try{error=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);}
        catch(const DbError& failure){state=failure.sqlState();handled=true;}
        std::cout<<"DELETE_BOUND_NATIVE_STATE "<<sql<<" STATE "<<state<<" ERROR "<<error<<std::endl;
        check("native owns exact AST "+sql,handled);
        check("native original state "+sql,state==expectedState && !error);
        check("ambient session restored "+sql,currentSession()==nullptr);
        return takeLastDmlResult();
    };
    auto result=run("DELETE FROM target AS victim WHERE victim.id=99 RETURNING abs(victim.id),victim.v,victim.\"V\",encode(victim.b,'hex'),victim.c,victim.a");
    check("zero match exact descriptor",result.available && !result.metadataOnly && result.rows.empty() && result.nulls.empty() &&
        result.commandTag=="DELETE 0" && result.columnTypes==std::vector<std::string>({"integer","text","text","text","character","integer[]"}));
    check("zero match all data",snapshot()==original);
    (void)run("DELETE FROM target WHERE FALSE RETURNING missing_delete_bound_function(id)","42883");
    check("failed static call no mutation",snapshot()==original && !g_engine.inTransaction());
    (void)run("DELETE FROM target WHERE id<=2 RETURNING 10/(id-2),nextval('effects')","22012");
    check("projection error same owner rollback",snapshot()==original && !g_engine.inTransaction());
    const auto effectValue=g_engine.nextval(database,"effects");
    std::cout<<"DELETE_BOUND_NATIVE_NEXTVAL "<<effectValue<<std::endl;
    check("projection volatile effect evaluated once",effectValue==2);
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(g_engine.updateRows(database,"target",{{"v",std::nullopt}},{"=id 3"})==DBStatus::OK);
    const auto parent=snapshot();
    (void)run("DELETE FROM target WHERE id<=2 RETURNING 10/(id-2)","22012");
    check("parent still active and prior NULL write retained",g_engine.inTransaction() && snapshot()==parent);
    assert(g_engine.commitTransaction()==DBStatus::OK);
    result=run("DELETE FROM target AS victim WHERE victim.id<=2 RETURNING abs(victim.id),victim.v,victim.\"V\",encode(victim.b,'hex'),victim.c,victim.a");
    check("actual function rows",result.available && result.commandTag=="DELETE 2" && result.rows==std::vector<std::vector<std::string>>({
        {"1","a","Upper","0041","a  ","{1,2}"},{"2","ab","NULL","ff","é  ","NULL"}}));
    check("actual NULL bitmap",result.nulls==std::vector<std::vector<bool>>({{false,false,false,false,false,false},{false,false,false,false,false,true}}));
    const auto remaining=snapshot();
    check("WHERE exact surviving rows",remaining.first.size()==3 && remaining.first[0][0]=="3" && remaining.second[0][1] &&
        remaining.first[0][2].empty() && !remaining.second[0][2] && remaining.first[1][0]=="4" &&
        remaining.second[1][1] && remaining.second[1][2] && remaining.first[2][1]=="NULL" && !remaining.second[2][1]);
    // The baseline records every preceding failure; native reseeding is
    // diagnostic cleanup only, not a replacement for any expected output.
    assert(g_engine.removeRows(database,"target",{})==DBStatus::OK);seed();
    result=run("DELETE FROM target WHERE v IS NULL OR v IN ('','NULL') RETURNING id,v");
    // Public bridge payload markers are compatibility spelling, not NULL
    // identity: row 4 is true NULL, row 5 remains four-byte text "NULL".
    check("NULL empty text NULL qualification",result.commandTag=="DELETE 3" && result.rows==std::vector<std::vector<std::string>>({{"3",""},{"4","NULL"},{"5","NULL"}}) &&
        result.nulls==std::vector<std::vector<bool>>({{false,false},{false,true},{false,false}}));
    check("qualification retained excluded rows",snapshot().first.size()==2);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"DELETE_BOUND_NATIVE_FAILURE_COUNT="<<failures.size()<<std::endl;
    assert(failures.empty());
}
