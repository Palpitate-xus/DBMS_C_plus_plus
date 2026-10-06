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
    const std::string name="insert_width_input_priority",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rows_table(id INT,v INTERVAL)",session));
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
        {"VALUES(CAST('1 fortnight' AS INTERVAL),2)","22007"},
        {"VALUES('2147483648 months',missing_width_function(1))","42883"},
        {"SELECT CAST('1 fortnight' AS INTERVAL),2","22007"},
        {"SELECT '2147483648 months',missing_width_function(1)","42883"},
        {"VALUES('2147483648 months',2)","42601"},
        {"VALUES(CAST(2147483648 AS INTEGER),2)","42601"},
        {"VALUES(1/0,2)","42601"},
        {"VALUES(INTERVAL '1 day',2)","42601"},
        {"VALUES('2147483648 months'),(missing_width_function(1),2)","22015"},
        {"VALUES('1 day'),(CAST('1 fortnight' AS INTERVAL),2)","22007"}}) {
        const std::string sql="INSERT INTO rows_table(v) "+item.first;
        bool handled=false; std::string actual;
        try { (void)tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql); }
        catch (const DbError& error) { actual=error.sqlState(); }
        if(actual!=item.second) std::cerr<<"INSERT_WIDTH "<<sql<<" actual "<<actual<<" expected "<<item.second<<'\n';
        assert(actual==item.second);
        assert(g_engine.query(db,"rows_table",{}, {"id"}).empty());
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[INSERT WIDTH INPUT PRIORITY] transform/width/context/planning order controls passed\n";
}
