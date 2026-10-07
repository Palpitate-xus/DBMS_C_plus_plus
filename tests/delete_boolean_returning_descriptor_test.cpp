#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="delete_boolean_returning_descriptor";
    cleanupTestDb(name); const auto database=testDbPath(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    setCurrentSession(&session); DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT,v VARCHAR(4)[],\"V\" CHAR(3)[])",session));
    assert(g_engine.insertRow(database,"t",{{"id","1"},{"v",std::nullopt},{"V",std::nullopt}})==DBStatus::OK);
    for(const auto* predicate:{"false","true","false"}) {
        const std::string sql=std::string("DELETE FROM t WHERE ")+predicate+" RETURNING v,\"V\"";
        bool handled=false;
        assert(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql));
        std::cerr<<"DELETE_BOOL_HANDLED "<<handled<<'\n';assert(handled);
        const auto output=takeLastDmlResult();
        assert(output.available && output.columns==std::vector<std::string>({"v","V"}));
        assert(output.columnTypes==std::vector<std::string>({"character varying[]","character[]"}));
        if(std::string(predicate)=="true") {
            assert(output.commandTag=="DELETE 1" && output.rows.size()==1);
            assert(output.nulls==std::vector<std::vector<bool>>({{true,true}}));
        } else assert(output.commandTag=="DELETE 0" && output.rows.empty() && output.nulls.empty());
    }
    assert(!ddl.executeSql("DROP TABLE t",session));
    setCurrentSession(nullptr);cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"[DELETE BOOLEAN RETURNING DESCRIPTOR] passed\n";
}
