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
    const std::string name = "update_typed_predicate", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rows_table(id INT PRIMARY KEY,v INT,b INT,t TEXT)", session));
    assert(g_engine.insertRow(db, "rows_table",
        {{"id", "1"}, {"v", "1"}, {"b", "10"}, {"t", std::string{}}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "rows_table",
        {{"id", "2"}, {"v", std::nullopt}, {"b", "20"}, {"t", std::nullopt}}) == DBStatus::OK);
    const auto run = [&](const std::string& sql) {
        bool handled = false;
        assert(!tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql));
        assert(handled);
    };
    const auto rows = [&] {
        std::vector<std::vector<std::string>> values;
        std::vector<std::vector<bool>> nulls;
        g_engine.query(db, "rows_table", {}, {"id", "v", "b", "t"},
                       {}, false, false, false, 0, {}, &values, &nulls);
        return std::make_pair(values, nulls);
    };
    run("UPDATE rows_table SET v=v+10 WHERE id=CAST('1' AS integer)");
    assert(g_engine.query(db, "rows_table", {"=v 11"}, {"id"}).size() == 1);
    run("UPDATE rows_table AS x SET v=v+10 WHERE CASE WHEN x.id=1 THEN true ELSE false END");
    assert(g_engine.query(db, "rows_table", {"=v 21"}, {"id"}).size() == 1);
    run("UPDATE rows_table SET v=v+10 WHERE v+1>0");
    assert(g_engine.query(db, "rows_table", {"=v 31"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "rows_table", {"isnull v"}, {"id"}).size() == 1);
    run("UPDATE rows_table SET v=b,b=v WHERE true");
    assert(g_engine.query(db, "rows_table", {"=id 1", "=v 10", "=b 31"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "rows_table", {"=id 2", "=v 20", "isnull b"}, {"id"}).size() == 1);
    run("UPDATE rows_table SET v=99 WHERE t=''");
    assert(g_engine.query(db, "rows_table", {"=v 99"}, {"id"}).size() == 1);
    run("UPDATE rows_table AS x SET v=(SELECT x.v+1) WHERE id=1");
    assert(g_engine.query(db, "rows_table", {"=id 1", "=v 100"}, {"id"}).size() == 1);
    run("UPDATE rows_table AS x SET v=v+1 WHERE (SELECT x.id)=1");
    assert(g_engine.query(db, "rows_table", {"=id 1", "=v 101"}, {"id"}).size() == 1);
    auto before = rows();
    run("UPDATE rows_table SET v=999 WHERE NULL");
    assert(rows() == before);
    const auto error = [&](const std::string& sql, const std::string& state) {
        bool precise = false, handled = false;
        try {
            (void)tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql);
        } catch (const DbError& failure) {
            precise = failure.sqlState() == state;
        }
        assert(precise);
        assert(rows() == before);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    error("UPDATE rows_table SET v=123 WHERE CAST(NULL AS text)", "42804");
    error("UPDATE rows_table SET v='bad' WHERE false", "22P02");
    error("UPDATE rows_table SET v=1 WHERE 'not_boolean'", "22P02");
    run("UPDATE rows_table SET id=NULL WHERE false");
    assert(rows() == before);
    error("UPDATE rows_table SET v=123 WHERE id=CAST('bad' AS integer)", "22P02");
    error("UPDATE rows_table SET v=CASE WHEN false THEN missing ELSE 1 END WHERE false", "42703");
    assert(g_engine.remove(db, "rows_table", {}) == DBStatus::OK);
    before = rows();
    error("UPDATE rows_table SET v=123 WHERE 1", "42804");
    error("UPDATE rows_table SET v=123 WHERE CAST(NULL AS text)", "42804");
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[UPDATE TYPED PREDICATE] CAST/CASE/NULL/OLD-row/empty-input/error controls passed\n";
}
