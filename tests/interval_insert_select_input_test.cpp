#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="interval_insert_select_input",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target_rows(id INT PRIMARY KEY,v INTERVAL)",session));
    assert(!ddl.executeSql("CREATE TABLE source_text(id INT,v TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE source_interval(id INT,v INTERVAL)",session));
    assert(g_engine.insertRow(db,"source_text",{{"id","1"},{"v","1 day"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"source_text",{{"id","2"},{"v",std::nullopt}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"source_interval",{{"id","1"},{"v","1 us"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"source_interval",{{"id","2"},{"v",std::nullopt}})==DBStatus::OK);
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
            {"SELECT 10,'2147483648 months' WHERE false","22015"},
            {"SELECT 10,'1 fortnight' WHERE false","22007"},
            {"SELECT 10,CAST('1 day' AS TEXT) WHERE false","42804"},
            {"SELECT 10,CAST(NULL AS TEXT) WHERE false","42804"},
            {"SELECT 10,1 WHERE false","42804"},
            {"SELECT id,v FROM source_text WHERE false","42804"},
            {"SELECT id,v FROM source_text","42804"},
            {"SELECT * FROM source_text WHERE false","42804"},
            {"SELECT 10,INTERVAL '2147483648 months' WHERE false","22015"},
            {"SELECT 10,CAST('2147483648 months' AS INTERVAL) WHERE false","22015"},
            {"SELECT 10,'2147483648 months'::INTERVAL WHERE false","22015"}}) {
        const std::string sql="INSERT INTO target_rows "+item.first;
        bool handled=false, precise=false;
        try { (void)tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql); }
        catch (const DbError& error) {
            precise=error.sqlState()==item.second;
            if (!precise) std::cerr<<"INSERT_SELECT "<<sql<<" actual "<<error.sqlState()
                                   <<" expected "<<item.second<<'\n';
        }
        if (!precise) std::cerr<<"missing static target coercion: "<<sql<<'\n';
        assert(precise);
        assert(g_engine.query(db,"target_rows",{}, {"id"}).empty());
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
    const auto run=[&](const std::string& source) {
        const std::string sql="INSERT INTO target_rows "+source;
        bool handled=false;
        assert(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql));
        assert(handled);
    };
    run("SELECT 10,'1 day' WHERE false");
    run("SELECT 10,NULL WHERE false");
    run("SELECT 10,CAST(NULL AS INTERVAL) WHERE false");
    run("SELECT 10,'1 us'");
    run("SELECT 11,NULL");
    run("SELECT id,v FROM source_interval");
    assert(g_engine.query(db,"target_rows",{"isnull v"},{"id"}).size()==2);
    assert(g_engine.query(db,"target_rows",{"isnotnull v"},{"id"}).size()==2);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"[INTERVAL INSERT SELECT INPUT] static zero-input/type/NULL/width controls passed\n";
}
