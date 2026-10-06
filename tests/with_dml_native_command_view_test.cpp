#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="with_native_command_view",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE target(id INT)",session));
    for(const bool explicitTransaction:{false,true}) {
        if(explicitTransaction) {
            assert(g_engine.beginTransaction(db)==DBStatus::OK);
            // Earlier successful native writes have no main/SPI command
            // owner, but must remain visible to this new logical command.
            assert(g_engine.insertRow(db,"target",{{"id","70"}})==DBStatus::OK);
        }
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
            "WITH first AS(INSERT INTO target VALUES(80) RETURNING id) INSERT INTO target VALUES(81)"));
        auto* with=static_cast<WithStmt*>(query->ast.get());
        executeAtomicDmlUnit(session,[&] {
            (void)executeBoundDml(with->ctes.front().query.get(),session,query,{});
            const auto visible=g_engine.query(db,"target",{"=id 80"},{"id"});
            std::cerr<<"native WITH fixed-view rows="<<visible.size()<<" expected 0\n";
            assert(visible.empty());
            if(explicitTransaction)assert(g_engine.query(db,"target",{"=id 70"},{"id"}).size()==1);
            return executeBoundDml(with->statement.get(),session,query,{});
        });
        assert(g_engine.query(db,"target",{"=id 80"},{"id"}).size()==1);
        if(explicitTransaction)assert(g_engine.rollbackTransaction()==DBStatus::OK);
        assert(g_engine.removeRows(db,"target",{})==DBStatus::OK);
    }
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[WITH NATIVE COMMAND VIEW] implicit/borrowed native units passed\n";
}
