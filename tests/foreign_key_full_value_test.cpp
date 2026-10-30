#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string database=testDbPath("fk_full_value");
    size_t controls=0,failures=0;
    const auto check=[&](bool pass,const std::string& label){
        ++controls;
        if(!pass){++failures;std::cerr<<"FK_FULL_VALUE_FAILURE "<<label<<'\n';}
    };
    check(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"owned database");
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    setCurrentSession(&session);
    struct Clear {~Clear(){setCurrentSession(nullptr);}} clear;
    struct Value {std::string parent,missing;};
    const std::vector<Value> values={
        {"abcdefghijklmnopqrstA","abcdefghijklmnopqrstB"},
        {"abcdefghijklmnopqrst","abcdefghijklmnopqrstZ"},
        {"abcdefghijklmnopqrs","abcdefghijklmnopqrst"},
        {"汉汉汉汉汉汉abA","汉汉汉汉汉汉abB"},
        {"","not-present"},
        {"NULL","NULL-but-not-null"},
        {std::string(127,'a')+"x",std::string(127,'a')+"y"}
    };
    for(size_t i=0;i<values.size();++i) {
        const auto parent="p"+std::to_string(i),child="c"+std::to_string(i);
        TableSchema referenced;referenced.tablename=parent;
        auto key=makeVarCharColumn("v",false,256);key.isPrimaryKey=true;
        referenced.append(key);
        check(g_engine.createTable(database,referenced)==DBStatus::OK,"parent schema");
        check(g_engine.insertRow(database,parent,{{"v",values[i].parent}})==DBStatus::OK,"parent row");
        TableSchema local;local.tablename=child;
        local.append(makeIntColumn("id",false,4,true));
        local.append(makeVarCharColumn("v",true,256));
        ForeignKey fk;fk.colNames={"v"};fk.refTable=parent;fk.refCols={"v"};
        local.appendFK(fk);
        check(g_engine.createTable(database,local)==DBStatus::OK,"child schema");
        check(g_engine.insertRow(database,child,{{"id",std::string("1")},{"v",values[i].missing}})==
            DBStatus::FOREIGN_KEY_VIOLATION,"prefix match is not a parent");
        check(g_engine.query(database,child,{},{"id"},{}).empty(),"rejected insert has no row");
        check(g_engine.insertRow(database,child,{{"id",std::string("2")},{"v",values[i].parent}})==
            DBStatus::OK,"exact parent succeeds");
        check(g_engine.insertRow(database,child,{{"id",std::string("3")},{"v",std::nullopt}})==
            DBStatus::OK,"SQL NULL does not require parent");
        check(g_engine.query(database,child,{},{"id"},{}).size()==2,"only exact and NULL rows");
    }
    struct TypedValue {Column column;std::string parent,missing;};
    const std::vector<TypedValue> typed={
        {makeINetColumn("v",false),"192.0.2.1","192.0.2.2"},
        {makeMacAddrColumn("v",false),"08-00-2b-01-02-03","08-00-2b-01-02-04"},
        {makeDateColumn("v",false),"2000-1-1","2000-1-2"},
        {makeTimestamptzColumn("v",false),"2000-01-01 00:00:00+00","2000-01-01 00:00:01+00"}
    };
    session.timeZone="America/New_York";session.timezoneOffsetMinutes=-300;
    for(size_t i=0;i<typed.size();++i) {
        const auto parent="typed_p"+std::to_string(i),child="typed_c"+std::to_string(i);
        TableSchema referenced;referenced.tablename=parent;
        Column key=typed[i].column;key.isPrimaryKey=true;referenced.append(key);
        check(g_engine.createTable(database,referenced)==DBStatus::OK,"typed parent schema");
        check(g_engine.insertRow(database,parent,{{"v",typed[i].parent}})==DBStatus::OK,"typed parent row");
        TableSchema local;local.tablename=child;local.append(makeIntColumn("id",false,4,true));
        key.isPrimaryKey=false;key.isNull=true;local.append(key);
        ForeignKey fk;fk.colNames={"v"};fk.refTable=parent;fk.refCols={"v"};local.appendFK(fk);
        check(g_engine.createTable(database,local)==DBStatus::OK,"typed child schema");
        check(g_engine.insertRow(database,child,{{"id",std::string("1")},{"v",typed[i].missing}})==
            DBStatus::FOREIGN_KEY_VIOLATION,"typed nonexistent parent rejected");
        check(g_engine.query(database,child,{},{"id"},{}).empty(),"typed rejection atomic");
        check(g_engine.insertRow(database,child,{{"id",std::string("2")},{"v",typed[i].parent}})==
            DBStatus::OK,"typed and alternate spelling exact parent accepted");
        check(g_engine.insertRow(database,child,{{"id",std::string("3")},{"v",std::nullopt}})==
            DBStatus::OK,"typed SQL NULL accepted");
        check(g_engine.query(database,child,{},{"id"},{}).size()==2,"typed exact and NULL rows only");
    }
    std::cout<<"FK_FULL_VALUE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return controls==89 && failures==0?0:1;
}
