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
    const std::string name="update_source_bound",database=testDbPath(name);cleanupTestDb(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,v TEXT,a INT[])",session));
    assert(!ddl.executeSql("CREATE TABLE source(id INT,v TEXT,a INT[])",session));
    assert(!ddl.executeSql("CREATE TABLE nullable(id INT,v TEXT)",session));
    assert(!ddl.executeSql("CREATE SEQUENCE effects",session));
    assert(g_engine.createIndex(database,"target","v")==DBStatus::OK);
    const std::vector<StorageEngine::SqlRow> originals={
        {{"id","1"},{"v","old1"},{"a","[0:1]={1,2}"}},
        {{"id","2"},{"v","old2"},{"a",std::nullopt}},
        {{"id","3"},{"v",""},{"a","{}"}},
        {{"id","4"},{"v",std::nullopt},{"a",std::nullopt}}
    };
    for(const auto& row:originals)assert(g_engine.insertRow(database,"target",row)==DBStatus::OK);
    for(const auto& row:std::vector<StorageEngine::SqlRow>{
        {{"id","1"},{"v","new1"},{"a","[2:3]={5,6}"}},
        {{"id","2"},{"v",std::nullopt},{"a",std::nullopt}},
        {{"id","3"},{"v","NULL"},{"a","{}"}},
        {{"id","4"},{"v",""},{"a",std::nullopt}}})
        assert(g_engine.insertRow(database,"source",row)==DBStatus::OK);
    assert(g_engine.insertRow(database,"nullable",{{"id","1"},{"v","right"}})==DBStatus::OK);
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&]{Snapshot value;(void)g_engine.query(database,"target",{},{"id","v","a"},{{"id",true}},false,false,false,0,{},&value.first,&value.second);return value;};
    const auto original=snapshot();std::vector<std::string> failures;
    const auto check=[&](const std::string& label,bool good){if(!good){failures.push_back(label);std::cerr<<"UPDATE_SOURCE_NATIVE_FAILURE "<<label<<'\n';}};
    const auto run=[&](const std::string& sql,const std::string& expected={}) {
        bool handled=false,error=false;std::string state;clearLastDmlResult();
        try{error=tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);}
        catch(const DbError& failure){state=failure.sqlState();handled=true;}
        std::cout<<"UPDATE_SOURCE_NATIVE_STATE "<<sql<<" STATE "<<state<<std::endl;
        check("owns exact AST "+sql,handled);check("exact SQLSTATE "+sql,state==expected && !error);
        check("ambient restored "+sql,currentSession()==nullptr);return takeLastDmlResult();
    };
    const auto ordered=[&](DmlResult value) {
        // RETURNING has no ORDER BY contract. A prior UPDATE may relocate a
        // physical RID. Sort complete row/NULL-bit pairs, never each channel
        // independently, and keep every original expected cell and bit.
        if(value.rows.size()!=value.nulls.size())return value;
        std::vector<std::pair<std::vector<std::string>,std::vector<bool>>> pairs;
        for(size_t i=0;i<value.rows.size();++i)pairs.emplace_back(value.rows[i],value.nulls[i]);
        std::sort(pairs.begin(),pairs.end());
        for(size_t i=0;i<pairs.size();++i){value.rows[i]=pairs[i].first;value.nulls[i]=pairs[i].second;}
        return value;
    };
    (void)run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE victim.id=producer.id RETURNING id","42702");
    check("pure ambiguity no effects",snapshot()==original && !g_engine.inTransaction());
    // Diagnostic restore follows the recorded negative controls; it is not
    // substituted for their original no-effect assertions on the old runtime.
    assert(g_engine.removeRows(database,"target",{})==DBStatus::OK);
    for(const auto& row:originals)assert(g_engine.insertRow(database,"target",row)==DBStatus::OK);
    auto result=run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE FALSE RETURNING abs(victim.id),victim.v,victim.a");
    check("zero descriptor",result.available && result.commandTag=="UPDATE 0" && result.rows.empty() && result.columnTypes==std::vector<std::string>({"integer","text","integer[]"}));
    (void)run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE FALSE RETURNING missing_update_source_function(victim.id)","42883");
    (void)run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE FALSE RETURNING 1/0","22012");
    check("pure preparation no source mutation",snapshot()==original && !g_engine.inTransaction());
    (void)run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING 10/(victim.id-2),nextval('effects')","22012");
    check("late projection owner rollback",snapshot()==original && !g_engine.inTransaction());
    const auto next=g_engine.nextval(database,"effects");check("projection once",next==2);
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(g_engine.updateRows(database,"target",{{"v",std::nullopt}},{"=id 3"})==DBStatus::OK);
    const auto prior=snapshot();
    (void)run("UPDATE target AS victim SET v=producer.v FROM source AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING 10/(victim.id-2)","22012");
    check("prior parent NULL write retained",g_engine.inTransaction() && snapshot()==prior);
    assert(g_engine.commitTransaction()==DBStatus::OK);
    result=ordered(run("UPDATE target AS victim SET v=producer.v,a=producer.a FROM source AS producer WHERE victim.id=producer.id RETURNING abs(victim.id),victim.v,victim.a"));
    check("actual NULL text empty bounds values",result.commandTag=="UPDATE 4" && result.rows==std::vector<std::vector<std::string>>({{"1","new1","[2:3]={5,6}"},{"2","NULL","NULL"},{"3","NULL","{}"},{"4","","NULL"}}));
    check("actual NULL identity",result.nulls==std::vector<std::vector<bool>>({{false,false,false},{false,true,true},{false,false,false},{false,false,true}}));
    result=ordered(run("UPDATE target AS victim SET v=COALESCE(rightrow.v,'outer') FROM source AS producer LEFT JOIN nullable AS rightrow ON producer.id=rightrow.id WHERE victim.id=producer.id AND rightrow.id IS NULL RETURNING victim.id,victim.v,rightrow.id,rightrow.v"));
    check("actual nullable outer source",result.commandTag=="UPDATE 3" && result.rows==std::vector<std::vector<std::string>>({{"2","outer","NULL","NULL"},{"3","outer","NULL","NULL"},{"4","outer","NULL","NULL"}}));
    check("outer source NULL bitmap",result.nulls==std::vector<std::vector<bool>>({{false,false,true,true},{false,false,true,true},{false,false,true,true}}));
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"UPDATE_SOURCE_NATIVE_FAILURE_COUNT="<<failures.size()<<std::endl;assert(failures.empty());
}
