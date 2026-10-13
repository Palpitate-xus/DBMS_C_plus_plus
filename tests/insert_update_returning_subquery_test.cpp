#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string testName = "insert_update_returning_subquery";
    const auto database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema table;
    table.tablename = "target";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeIntColumn("v", true, 4));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    Session session;
    session.permission = 1;
    session.username = "testuser";
    session.currentDB = database;
    std::vector<std::string> failures;
    DmlResult lastResult;
    const auto lastDmlResult = [&]() -> const DmlResult& { return lastResult; };
    const auto check = [&](const std::string& name, bool good) {
        if (!good) {
            failures.push_back(name);
            std::cerr << "RETURNING_CHILD_FAILURE " << name << '\n';
        }
    };
    const auto run = [&](const std::string& sql) {
        bool handled = false;
        bool error;
        try {
            error = tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql);
            lastResult = takeLastDmlResult();
        } catch (...) {
            lastResult = takeLastDmlResult();
            throw;
        }
        check("bridge handled " + sql, handled);
        return error;
    };
    const auto read = [&] {
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        (void)g_engine.query(database, "target", {}, {"id", "v"}, {{"id", true}},
                            false, false, false, 0, {}, &rows, &nulls);
        return std::make_pair(rows, nulls);
    };
    const auto reset = [&] {
        assert(!g_engine.inTransaction());
        assert(g_engine.remove(database, "target", {}) == DBStatus::OK);
        for (const std::string id : {"1", "2", "3"})
            assert(g_engine.insert(database, "target", {{"id", id}, {"v", id + "0"}}) == DBStatus::OK);
        clearLastDmlResult();
        lastResult = {};
    };
    const std::vector<std::pair<std::string, std::string>> rejected = {
        {"INSERT INTO target VALUES(4,40) RETURNING id,(SELECT 1/0)", "22012"},
        {"UPDATE target SET v=99 WHERE id=1 RETURNING id,(SELECT 1/0)", "22012"},
        {"INSERT INTO target VALUES(4,40) RETURNING (SELECT v FROM target)", "21000"},
        {"UPDATE target SET v=99 WHERE id=1 RETURNING (SELECT v FROM target)", "21000"},
        {"INSERT INTO target VALUES(4,40),(5,50) RETURNING id,(SELECT 100/(id-5))", "22012"},
        {"UPDATE target SET v=v+1 WHERE id<=2 RETURNING id,(SELECT 100/(id-2))", "22012"},
        {"INSERT INTO target DEFAULT VALUES RETURNING (SELECT 1/0)", "22012"},
    };
    for (const auto& test : rejected) {
        reset();
        const auto original = read();
        std::string state;
        try { (void)run(test.first); }
        catch (const DbError& error) {
            state = error.sqlState();
            std::cout << "RETURNING_CHILD_ERROR " << state << ' ' << error.what() << '\n';
        }
        check("exact SQLSTATE " + test.first, state == test.second);
        check("implicit owner released " + test.first, !g_engine.inTransaction());
        check("all original rows and NULLs retained " + test.first, read() == original);
        check("no partial RETURNING result " + test.first, !lastDmlResult().available);
        check("new indexed row absent " + test.first,
              g_engine.query(database, "target", {"=id 4"}, {"id"}).empty());
    }
    reset();
    check("correlated INSERT success", !run(
        "INSERT INTO target VALUES(4,40) RETURNING id,(SELECT id+1) AS child,(SELECT NULL::INT) AS nullable"));
    for (const auto& type : lastDmlResult().columnTypes)
        std::cout << "RETURNING_CHILD_TYPE " << type << '\n';
    check("correlated INSERT exact descriptor", lastDmlResult().columns == std::vector<std::string>{"id", "child", "nullable"} &&
          // makeIntColumn scale=4 is the native BIGINT constructor (scale=2
          // would be INT). The child arithmetic must retain that exact type.
          lastDmlResult().columnTypes == std::vector<std::string>{"bigint", "bigint", "integer"});
    check("correlated INSERT exact cells", lastDmlResult().rows == std::vector<std::vector<std::string>>{{"4", "5", "NULL"}} &&
          lastDmlResult().nulls == std::vector<std::vector<bool>>{{false, false, true}});
    check("old/new scalar UPDATE success", !run(
        "UPDATE target SET v=99 WHERE id=1 RETURNING old.v,new.v,(SELECT v),(SELECT old.v)"));
    check("old/new scalar UPDATE cells", lastDmlResult().rows == std::vector<std::vector<std::string>>{{"10", "99", "99", "10"}});
    const auto prior = read();
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.insert(database, "target", {{"id", "99"}, {"v", "990"}}) == DBStatus::OK);
    const auto xid = g_engine.currentTxnId();
    std::string state;
    try { (void)run("UPDATE target SET v=999 WHERE id=1 RETURNING (SELECT v FROM target)"); }
    catch (const DbError& error) { state = error.sqlState(); }
    check("explicit cardinality error", state == "21000");
    check("explicit owner retained", g_engine.inTransaction() && g_engine.currentTxnId() == xid);
    check("earlier explicit write retained", g_engine.query(database, "target", {"=id 99"}, {"v"}).size() == 1);
    check("failed update rolled back", g_engine.query(database, "target", {"=id 1"}, {"v"}) == std::vector<std::string>{"99 "});
    check("explicit no partial result", !lastDmlResult().available);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.remove(database, "target", {"=id 99"}) == DBStatus::OK);
    check("earlier commit and failed update leave original image", read() == prior);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "RETURNING_CHILD_FAILURE_COUNT=" << failures.size() << std::endl;
    assert(failures.empty());
}
