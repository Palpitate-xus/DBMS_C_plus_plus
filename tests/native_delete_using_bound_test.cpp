#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const std::string name="native_delete_using_bound",database=testDbPath(name);cleanupTestDb(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,v TEXT,a INT[])",session));
    assert(!ddl.executeSql("CREATE TABLE source(id INT,v TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE nullable(id INT,v TEXT)",session));
    assert(!ddl.executeSql("CREATE SEQUENCE effects",session));
    assert(g_engine.createIndex(database,"target","v")==DBStatus::OK);
    const std::vector<StorageEngine::SqlRow> originals={
        {{"id","1"},{"v","a"},{"a","[0:1]={1,2}"}},{{"id","2"},{"v","ab"},{"a",std::nullopt}},
        {{"id","3"},{"v",""},{"a","{}"}},{{"id","4"},{"v",std::nullopt},{"a",std::nullopt}},{{"id","5"},{"v","NULL"},{"a",std::nullopt}}};
    const auto seed=[&]{for(const auto& row:originals)assert(g_engine.insertRow(database,"target",row)==DBStatus::OK);};seed();
    for(const auto& row:std::vector<StorageEngine::SqlRow>{{{"id","1"},{"v","source"}},{{"id","1"},{"v","source"}},{{"id","2"},{"v",std::nullopt}},{{"id","4"},{"v","NULL"}}})
        assert(g_engine.insertRow(database,"source",row)==DBStatus::OK);
    assert(g_engine.insertRow(database,"nullable",{{"id","1"},{"v","right"}})==DBStatus::OK);
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&]{Snapshot value;(void)g_engine.query(database,"target",{},{"id","v","a"},{{"id",true}},false,false,false,0,{},&value.first,&value.second);return value;};
    const auto original=snapshot();std::vector<std::string> failures;
    const auto check=[&](const std::string& label,bool good){if(!good){failures.push_back(label);std::cerr<<"NATIVE_USING_FAILURE "<<label<<'\n';}};
    const auto run=[&](const std::string& sql,const std::string& expected={}) {
        bool handled=false,error=false;std::string state;clearLastDmlResult();
        try{error=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);}
        catch(const DbError& failure){handled=true;state=failure.sqlState();}
        std::cout<<"NATIVE_USING_STATE "<<sql<<" STATE "<<state<<std::endl;
        check("owns AST "+sql,handled);check("exact SQLSTATE "+sql,state==expected && !error);
        check("ambient restored "+sql,currentSession()==nullptr);return takeLastDmlResult();
    };
    const auto ordered=[](DmlResult value) {
        if(value.rows.size()!=value.nulls.size())return value;
        std::vector<std::pair<std::vector<std::string>,std::vector<bool>>> pairs;
        for(size_t i=0;i<value.rows.size();++i)pairs.emplace_back(value.rows[i],value.nulls[i]);
        std::sort(pairs.begin(),pairs.end());
        for(size_t i=0;i<pairs.size();++i){value.rows[i]=pairs[i].first;value.nulls[i]=pairs[i].second;}return value;
    };
    auto result=run("DELETE FROM target AS victim USING source AS producer WHERE FALSE RETURNING abs(victim.id),producer.v,victim.a");
    check("zero descriptor",result.available && result.commandTag=="DELETE 0" && result.rows.empty() && result.columnTypes==std::vector<std::string>({"integer","text","integer[]"}));
    (void)run("DELETE FROM target AS victim USING source AS producer WHERE FALSE RETURNING missing_native_using_function(victim.id)","42883");
    (void)run("DELETE FROM target AS victim USING source AS producer WHERE victim.id=producer.id RETURNING id","42702");
    check("pure errors no effects",snapshot()==original && !g_engine.inTransaction());
    // Restore only after recording baseline effects; the original no-effect
    // assertions are never replaced by this diagnostic reseed.
    assert(g_engine.removeRows(database,"target",{})==DBStatus::OK);seed();
    (void)run("DELETE FROM target AS victim USING source AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING 10/(victim.id-2),nextval('effects')","22012");
    check("late rollback all rows",snapshot()==original && !g_engine.inTransaction());
    check("projection once despite duplicate source",g_engine.nextval(database,"effects")==2);
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(g_engine.updateRows(database,"target",{{"v",std::nullopt}},{"=id 3"})==DBStatus::OK);const auto prior=snapshot();
    (void)run("DELETE FROM target AS victim USING source AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING 10/(victim.id-2)","22012");
    check("prior parent NULL retained",g_engine.inTransaction() && snapshot()==prior);assert(g_engine.commitTransaction()==DBStatus::OK);
    result=ordered(run("DELETE FROM target AS victim USING source AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING abs(victim.id),victim.v,producer.id,producer.v,victim.a"));
    check("actual source rows and bounds",result.commandTag=="DELETE 2" && result.rows==std::vector<std::vector<std::string>>({{"1","a","1","source","[0:1]={1,2}"},{"2","ab","2","NULL","NULL"}}));
    check("source NULL bitmap",result.nulls==std::vector<std::vector<bool>>({{false,false,false,false,false},{false,false,false,true,true}}));
    result=run("DELETE FROM target AS victim USING source AS producer LEFT JOIN nullable AS rightrow ON producer.id=rightrow.id WHERE victim.id=producer.id AND rightrow.id IS NULL RETURNING abs(victim.id),victim.v,producer.v,rightrow.v");
    check("nullable source actual cells",result.commandTag=="DELETE 1" && result.rows==std::vector<std::vector<std::string>>({{"4","NULL","NULL","NULL"}}));
    check("NULL and ordinary text NULL separate",result.nulls==std::vector<std::vector<bool>>({{false,true,false,true}}));
    const auto survivors=snapshot();check("all excluded rows retained",survivors.first.size()==2 && survivors.first[0][0]=="3" && survivors.second[0][1] && survivors.first[1][0]=="5" && survivors.first[1][1]=="NULL" && !survivors.second[1][1]);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"NATIVE_USING_FAILURE_COUNT="<<failures.size()<<std::endl;assert(failures.empty());
}
