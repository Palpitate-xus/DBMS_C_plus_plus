#include "commands/DdlExecutor.h"
#include "commands/DdlRewrite.h"
#include "commands/DdlTransaction.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <map>
#include <optional>

extern dbms::StorageEngine g_engine;
using namespace dbms;
using Rows = std::map<std::string,StorageEngine::SqlRow>;

static Rows snapshot(const std::string& database, const std::string& name) {
    const auto table = g_engine.getTableSchema(database,name);
    Rows rows;
    assert(g_engine.forEachRow(database,name,[&](uint32_t page,uint16_t slot,const char* data,size_t length) {
        StorageEngine::SqlRow row;
        const auto rid = StorageEngine::encodeRid(page,slot);
        for(size_t i=0;i<table.len;++i) {
            if(g_engine.isColumnNullByRid(database,name,rid,i)) row[table.cols[i].dataName]=std::nullopt;
            else row[table.cols[i].dataName]=g_engine.extractColumnValue(std::string(data,length),table,i,database);
        }
        assert(row.at("id"));
        assert(rows.emplace(*row.at("id"),row).second);
    }));
    return rows;
}

struct Fixture {
    std::string database;
    Session session;
    DdlExecutor ddl;
    Rows original;
    explicit Fixture(const std::string& suffix) : database(testDbPath("ddl_rewrite_owner_"+suffix)) {
        assert(g_engine.createDatabase(database)==DBStatus::OK);
        session.currentDB=database;session.username="admin";session.permission=1;
        assert(!ddl.executeSql("CREATE TABLE rows(id INT PRIMARY KEY,v INT,t TEXT)",session));
        assert(!ddl.executeSql("CREATE TABLE audit(id INT PRIMARY KEY)",session));
        original={
            {"1",{{"id","1"},{"v",std::nullopt},{"t",std::nullopt}}},
            {"2",{{"id","2"},{"v","0"},{"t",""}}},
            {"3",{{"id","3"},{"v","-2147483648"},{"t","NULL"}}},
            {"4",{{"id","4"},{"v","7"},{"t","tail"}}}};
        for(const auto& entry:original)assert(g_engine.insertRow(database,"rows",entry.second)==DBStatus::OK);
        assert(snapshot(database,"rows")==original);
    }
    ~Fixture() {
        g_engine.setTriggerExecutor({});
        if(g_engine.inTransaction())assert(g_engine.rollbackTransaction()==DBStatus::OK);
        assert(g_engine.dropDatabase(database)==DBStatus::OK);
    }
};

static void failure(bool outerTransaction) {
    Fixture fixture(outerTransaction?"outer":"implicit");
    const auto& database=fixture.database;
    assert(g_engine.createTrigger(database,{"rewrite_failure","after","insert","rows",
        "fail_rewrite","",true,true,{}})==DBStatus::OK);
    if(outerTransaction) {
        assert(g_engine.beginTransaction(database)==DBStatus::OK);
        assert(g_engine.insertRow(database,"audit",{{"id","99"}})==DBStatus::OK);
        assert(g_engine.savepoint("before_rewrite")==DBStatus::OK);
    }
    size_t calls=0;
    g_engine.setTriggerExecutor([&](const std::string& action) {
        assert(action=="fail_rewrite");++calls;
        // This is an ordinary, trigger-issued INSERT. It must retain its
        // own savepoint despite the surrounding optimized heap rewrite.
        assert(g_engine.insertRow(database,"audit",{{"id",std::to_string(calls)}})==DBStatus::OK);
        if(calls==3)throw DbError("P0001","original rewrite trigger failure");
        return false;
    });
    bool caught=false;
    try { (void)fixture.ddl.executeSql("ALTER TABLE rows ADD COLUMN added INT DEFAULT 5",fixture.session); }
    catch(const DbError& error) { caught=error.sqlState()=="P0001" && error.message()=="original rewrite trigger failure"; }
    g_engine.setTriggerExecutor({});
    assert(caught && calls==3);
    assert(snapshot(database,"rows")==fixture.original);
    assert(g_engine.getTableSchema(database,"rows").len==3);
    assert(!detail::ownsDdlRewrite(g_engine,database,"rows"));
    if(outerTransaction) {
        assert(g_engine.inTransaction());
        assert(snapshot(database,"audit")==Rows({{"99",{{"id","99"}}}}));
        assert(g_engine.rollbackToSavepoint("before_rewrite")==DBStatus::OK);
        assert(g_engine.releaseSavepoint("before_rewrite")==DBStatus::OK);
        assert(g_engine.commitTransaction()==DBStatus::OK);
        assert(snapshot(database,"audit")==Rows({{"99",{{"id","99"}}}}));
    } else {
        assert(!g_engine.inTransaction());
        assert(snapshot(database,"audit").empty());
    }
    assert(g_engine.getLockManager().lockedTables().empty());
    // A later native INSERT still follows its normal statement contract.
    assert(g_engine.insertRow(database,"rows",{{"id","5"},{"v","9"},{"t","later"}})==DBStatus::OK);
    assert(snapshot(database,"rows").size()==5);
}

int main() {
    TypeRegistry::instance().bootstrap();
    {
        Fixture fixture("success");
        DdlTransaction inactive(g_engine,fixture.database);
        bool rejected=false;
        try {detail::DdlRewriteScope invalid(inactive,g_engine,fixture.database,"rows");}
        catch(const DbError& error){rejected=error.sqlState()=="XX000";}
        assert(rejected && !g_engine.inTransaction());
        assert(!fixture.ddl.executeSql("ALTER TABLE rows ADD COLUMN added INT DEFAULT 5",fixture.session));
        auto expected=fixture.original;
        for(auto& entry:expected)entry.second["added"]="5";
        assert(snapshot(fixture.database,"rows")==expected);
        assert(!fixture.ddl.executeSql("ALTER TABLE rows ALTER COLUMN v TYPE BIGINT",fixture.session));
        assert(snapshot(fixture.database,"rows")==expected);
        assert(!fixture.ddl.executeSql("ALTER TABLE rows DROP COLUMN added",fixture.session));
        assert(snapshot(fixture.database,"rows")==fixture.original);
        assert(!detail::ownsDdlRewrite(g_engine,fixture.database,"rows"));
        // Direct native rewrites without a DdlTransaction scope keep their
        // existing per-row INSERT path.
        assert(g_engine.alterTableAddColumn(fixture.database,"rows",makeIntColumn("native",true,4))==DBStatus::OK);
        for(auto& entry:expected){entry.second.erase("added");entry.second["native"]=std::nullopt;}
        assert(snapshot(fixture.database,"rows")==expected);
    }
    failure(false);
    failure(true);
    std::cout<<"[DDL REWRITE OWNER] successful/native rewrites, trigger failure, outer prefix and savepoint rollback passed\n";
}
