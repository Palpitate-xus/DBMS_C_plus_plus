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
    const std::string name="with_bound_dml", db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session; session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,a INT,b INT,t TEXT)",session));
    const auto execute=[&](const std::string& sql, PreparedChildExecutor reader={}) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto* envelope=dynamic_cast<WithStmt*>(query->ast.get());
        assert(envelope && envelope->statement);
        prepareBoundDml(envelope->statement.get(),session,query);
        return executeAtomicDmlUnit(session,[&] {
            return executeBoundDml(envelope->statement.get(),session,query,std::move(reader));
        });
    };
    auto result=execute("WITH target AS(SELECT 99 AS id) INSERT INTO target VALUES(1,10,20,'') RETURNING id,a,b,t");
    assert(result.commandTag=="INSERT 0 1");
    assert((result.rows==std::vector<std::vector<std::string>>{{"1","10","20",""}}));
    assert(!result.nulls.front().back());
    result=execute("WITH c AS(SELECT 1) INSERT INTO target VALUES(2,NULL,30,NULL),(3,40,50,'NULL') RETURNING id,t");
    assert(result.rows.size()==2 && result.nulls[0][1]);
    assert(result.rows[1][1]=="NULL" && !result.nulls[1][1]);
    result=execute("WITH c AS(SELECT 1) UPDATE target AS x SET a=x.b,b=x.a WHERE x.id=1 RETURNING id,a,b,t");
    assert((result.rows==std::vector<std::vector<std::string>>{{"1","20","10",""}}));
    result=execute("WITH target AS(SELECT 999 AS id) DELETE FROM target WHERE id=2 RETURNING id,t");
    assert(result.rows.size()==1 && result.rows[0][0]=="2" && result.nulls[0][1]);
    result=execute("WITH c AS(SELECT 1) UPDATE target SET a=99 WHERE NULL RETURNING id");
    assert(result.commandTag=="UPDATE 0" && result.rows.empty());
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "WITH c AS(SELECT 1) INSERT INTO target(id,t) SELECT 4,NULL::TEXT RETURNING id,t"));
    auto* envelope=static_cast<WithStmt*>(query->ast.get());
    auto* insert=static_cast<InsertStmt*>(envelope->statement.get());
    size_t calls=0;
    PreparedChildExecutor reader=[&](const Stmt* statement,const RowContext&,size_t demand) {
        assert(statement==insert->selectSource.get() && demand==0); ++calls;
        return PreparedQueryRows{{ExprValue("integer","4",false),ExprValue("text","",true)}};
    };
    result=executeAtomicDmlUnit(session,[&]{return executeBoundDml(insert,session,query,reader);});
    assert(calls==1 && result.rows.size()==1 && result.nulls[0][1]);
    const auto state=[&](const std::string& sql,const std::string& expected) {
        bool precise=false;
        try{execute(sql);}catch(const DbError& error){precise=error.sqlState()==expected;}
        assert(precise);
        assert(!g_engine.inTransaction());
    };
    state("WITH c AS(SELECT 1) UPDATE target SET a=9 WHERE CAST(NULL AS TEXT)","42804");
    state("WITH c AS(SELECT 1) UPDATE target SET a=9 WHERE id=CAST('bad' AS INT)","22P02");
    // A failure after a genuine earlier DML child must roll back the whole
    // WITH unit while preserving a user's successful command/savepoint cut.
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insertRow(db,"target",{{"id","90"}})==DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.savepoint("keep")==DBStatus::OK);
    auto failed=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "WITH first AS(INSERT INTO target(id) VALUES(91) RETURNING id) INSERT INTO target(id) VALUES(90)"));
    auto* whole=static_cast<WithStmt*>(failed->ast.get());
    bool duplicate=false;
    try {
        assert(g_engine.beginSqlCommand());
        executeAtomicDmlUnit(session,[&] {
            (void)executeBoundDml(whole->ctes.front().query.get(),session,failed,{});
            return executeBoundDml(whole->statement.get(),session,failed,{});
        });
    } catch(const DbError& error){duplicate=error.sqlState()=="23505";}
    assert(duplicate && g_engine.inTransaction());
    assert(g_engine.rollbackToSavepoint("keep")==DBStatus::OK);
    assert(g_engine.query(db,"target",{"=id 90"},{"id"}).size()==1);
    assert(g_engine.query(db,"target",{"=id 91"},{"id"}).empty());
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(g_engine.query(db,"target",{"=id 90"},{"id"}).empty());
    state("WITH c AS(SELECT 1) UPDATE target SET a=1,\"a\"=2 WHERE false", "42601");
    state("WITH c AS(SELECT 1) UPDATE target SET a=1,a=2 WHERE false", "42601");
    state("WITH c AS(SELECT 1) UPDATE target SET a=1/0,\"a\"=2 WHERE false", "42601");
    // The new unit entry is also callable without main's existing SQL
    // command. It must establish one fixed command view for the entire
    // unit, not let a base scan refresh after an earlier CTE write.
    auto native=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "WITH first AS(INSERT INTO target(id) VALUES(80) RETURNING id) INSERT INTO target(id) VALUES(81)"));
    auto* primary=static_cast<WithStmt*>(native->ast.get());
    executeAtomicDmlUnit(session,[&] {
        (void)executeBoundDml(primary->ctes.front().query.get(),session,native,{});
        const auto visible=g_engine.query(db,"target",{"=id 80"},{"id"});
        std::cerr<<"native WITH earlier-command rows="<<visible.size()<<" expected 0\n";
        assert(visible.empty());
        return executeBoundDml(primary->statement.get(),session,native,{});
    });
    assert(g_engine.query(db,"target",{"=id 80"},{"id"}).size()==1);
    assert(g_engine.query(db,"target",{"=id 81"},{"id"}).size()==1);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK); cleanupTestDb(name);
    std::cout<<"[WITH BOUND DML] owner/NULL/OLD-row/physical-target/atomic boundary passed\n";
}
