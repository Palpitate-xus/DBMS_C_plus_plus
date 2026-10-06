#include "Session.h"
#include "commands/DdlExecutor.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name="prepared_unknown_input", db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT,a INT)",session));
    const auto rejected=[&](const std::string& expression,const std::string& state) {
        bool precise=false;
        try { (void)g_engine.prepareBoundQuery(db,"WITH unused AS(SELECT "+expression+
              ") INSERT INTO target VALUES(1,1)"); }
        catch(const DbError& error) {
            std::cerr<<expression<<" actual="<<error.sqlState()<<" expected="<<state<<'\n';
            precise=error.sqlState()==state;
        }
        assert(precise);
        assert(!g_engine.inTransaction());
        assert(g_engine.query(db,"target",{}, {"id"}).empty());
    };
    for(const auto& [expression,state] : std::vector<std::pair<std::string,std::string>>{
        {"CAST('bad' AS INT)","22P02"}, {"'bad'::INT","22P02"},
        {"CAST('1.2' AS INT)","22P02"}, {"CAST('32768' AS SMALLINT)","22003"},
        {"CAST('9223372036854775808' AS BIGINT)","22003"},
        {"CAST('not-bool' AS BOOLEAN)","22P02"}, {"CAST('bad' AS NUMERIC)","22P02"},
        {"CAST('bad' AS REAL)","22P02"}, {"CAST('bad' AS DOUBLE PRECISION)","22P02"},
        {"CAST('bad' AS UUID)","22P02"}}) rejected(expression,state);
    rejected("CAST('bad' AS pg_catalog.int4)","22P02");
    rejected("CAST('bad' AS \"pg_catalog\".\"int4\")","22P02");
    rejected("BOOLEAN 'not-bool'","22P02");
    rejected("NUMERIC 'bad'","22P02");
    rejected("CAST($value$bad$value$ AS INT)","22P02");
    rejected("CAST(E'b\\x61d' AS INT)","22P02");
    for(const auto& expression : {"1/0","CAST(2147483648 AS INT)","CAST('bad' AS TEXT)::INT",
        "CAST('9999' AS NUMERIC(2,0))","CAST(NULL AS INT)","CAST('1.2' AS NUMERIC)::INT",
        "CAST($value$12$value$ AS INT)","CAST(E'\\x31\\x32' AS INT)"}) {
        (void)g_engine.prepareBoundQuery(db,"WITH unused AS(SELECT "+std::string(expression)+
              ") INSERT INTO target VALUES(1,1)");
        assert(!g_engine.inTransaction());
    }
    // Runtime parameters are typed cells, not unknown input constants.
    auto query=g_engine.prepareBoundQuery(db,"WITH unused AS(SELECT CAST($1 AS INT)) INSERT INTO target VALUES(1,1)",
        {{"p","arg1","text",{},false,"bad",1}});
    assert(query.parameters.size()==1 && query.parameters.front().value=="bad");
    assert(!g_engine.inTransaction());
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[PREPARED UNKNOWN INPUT] metadata-only conversion passed\n";
}
