#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="with_returning_transition",db=testDbPath(name);
    cleanupTestDb(name); assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,v INT,t TEXT,\"V\" BIGINT)",session));
    const auto execute=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto* envelope=dynamic_cast<WithStmt*>(query->ast.get());
        assert(envelope && envelope->statement);
        prepareBoundDml(envelope->statement.get(),session,query);
        return executeAtomicDmlUnit(session,[&] {
            return executeBoundDml(envelope->statement.get(),session,query,{});
        });
    };
    auto result=execute("WITH c AS(SELECT 1) INSERT INTO target VALUES(1,10,'',2147483648),(2,NULL,'NULL',NULL) RETURNING old.id,new.id,old.v,new.v,old.t,new.t,old.\"V\",new.\"V\"");
    assert(result.commandTag=="INSERT 0 2");
    assert((result.columnTypes==std::vector<std::string>{"integer","integer","integer","integer","text","text","bigint","bigint"}));
    assert(result.rows.size()==2 && result.rows[0][1]=="1" && result.rows[0][3]=="10" && result.rows[0][5].empty() && result.rows[0][7]=="2147483648");
    assert((result.nulls[0]==std::vector<bool>{true,false,true,false,true,false,true,false}));
    assert((result.nulls[1]==std::vector<bool>{true,false,true,true,true,false,true,true}));
    assert(result.rows[1][5]=="NULL");
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=1 RETURNING old.v,new.v,v");
    assert((result.rows==std::vector<std::vector<std::string>>{{"10","11","11"}}));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS o,NEW AS \"O\") o.v+\"O\".v,o.t,\"O\".\"V\"");
    assert((result.rows==std::vector<std::vector<std::string>>{{"23","","2147483648"}}));
    assert((result.columnTypes==std::vector<std::string>{"integer","text","bigint"}));
    result=execute("WITH c AS(SELECT 1) UPDATE target AS old SET v=v+1 WHERE id=1 RETURNING old.v,new.v,v");
    assert((result.rows==std::vector<std::vector<std::string>>{{"13","13","13"}}));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS new) new.v,v");
    assert((result.rows==std::vector<std::vector<std::string>>{{"13","14"}}));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=1 RETURNING WITH(NEW AS old) old.v,v");
    assert((result.rows==std::vector<std::vector<std::string>>{{"15","15"}}));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=1 RETURNING WITH(OLD AS \"o.x\",NEW AS \"n.x\") \"o.x\".*,\"n.x\".*");
    assert((result.rows==std::vector<std::vector<std::string>>{{"1","15","","2147483648","1","16","","2147483648"}}));
    assert(result.nulls.front()==std::vector<bool>(8,false));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=v+1 WHERE id=2 RETURNING old.v,new.v,old.t,new.t");
    assert(result.rows.size()==1 && result.nulls[0][0] && result.nulls[0][1] && result.rows[0][2]=="NULL" && result.rows[0][3]=="NULL");
    result=execute("WITH c AS(SELECT 1) DELETE FROM target WHERE id=2 RETURNING old.*,new.*");
    assert(result.commandTag=="DELETE 1" && result.rows.size()==1 && result.rows[0][0]=="2");
    assert((result.nulls[0]==std::vector<bool>{false,true,false,true,true,true,true,true}));
    result=execute("WITH c AS(SELECT 1) UPDATE target SET v=99 WHERE false RETURNING old.v,new.v");
    assert(result.commandTag=="UPDATE 0" && result.rows.empty() && result.columnTypes==std::vector<std::string>({"integer","integer"}));
    result=execute("WITH c AS(SELECT 1) DELETE FROM target WHERE false RETURNING old.*,new.*");
    assert(result.commandTag=="DELETE 0" && result.rows.empty() && result.columnTypes.size()==8);
    for(const auto& [sql,state]:std::vector<std::pair<std::string,std::string>>{
        {"WITH c AS(SELECT 1) INSERT INTO target(id) VALUES(3) RETURNING old.missing","42703"},
        {"WITH c AS(SELECT 1) UPDATE target SET v=99 RETURNING WITH(OLD AS before_row) old.v","42P01"},
        {"WITH c AS(SELECT 1) DELETE FROM target RETURNING WITH(OLD AS x,NEW AS x) x.id","42712"}}) {
        bool precise=false;
        try{execute(sql);}catch(const DbError& error){precise=error.sqlState()==state;}
        assert(precise && !g_engine.inTransaction());
    }
    assert(g_engine.query(db,"target",{}, {"id"}).size()==1);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[WITH RETURNING TRANSITIONS] actual ordinal channels, widths/NULLs, aliases/star and empty demand passed\n";
}
