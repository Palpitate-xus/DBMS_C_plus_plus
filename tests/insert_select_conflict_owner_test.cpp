#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture = "insert_select_conflict_owner", database = testDbPath(fixture);
    cleanupTestDb(fixture);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    for (const auto& name : {"target", "source_rows"}) {
        TableSchema table;
        table.tablename = name;
        table.append(makeIntColumn("id", false, 2, true));
        table.append(makeIntColumn("v", true, 2));
        assert(g_engine.createTable(database, table) == DBStatus::OK);
    }
    assert(g_engine.insertRow(database, "source_rows", {{"id", "1"}, {"v", "20"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "source_rows", {{"id", "2"}, {"v", "30"}}) == DBStatus::OK);
    Session session;
    session.permission = 1;
    session.username = "admin";
    session.currentDB = database;
    size_t checks = 0, failures = 0;
    DmlResult result;
    const auto check = [&](bool good, const std::string& label) {
        ++checks;
        if (!good) { ++failures; std::cerr << "INSERT_SOURCE_OWNER_FAILURE " << label << '\n'; }
    };
    const auto run = [&](const std::string& sql) {
        clearLastDmlResult();
        bool handled = false;
        std::string state;
        try {
            if (tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql)) state = "unstructured-error";
            else if (!handled) state = "legacy-fallback";
        } catch (const DbError& error) { state = error.sqlState(); }
        result = takeLastDmlResult();
        std::cout << "INSERT_SOURCE_OWNER " << sql << " STATE " << state << std::endl;
        return state;
    };
    const auto reset = [&] {
        assert(!g_engine.inTransaction());
        assert(g_engine.remove(database, "target", {}) == DBStatus::OK);
        assert(g_engine.insertRow(database, "target", {{"id", "1"}, {"v", "10"}}) == DBStatus::OK);
    };
    struct Control { std::string source, action; std::vector<std::vector<std::string>> rows; };
    for (const auto& control : std::vector<Control>{
             {"SELECT 1,20", "ON CONFLICT(id) DO UPDATE SET v=excluded.v", {{"1", "20"}}},
             {"SELECT 1,20", "ON CONFLICT(id) DO NOTHING", {}},
             {"SELECT 2,30", "ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE TRUE", {{"2", "30"}}},
             {"SELECT (SELECT 2),30 WHERE FALSE", "ON CONFLICT(id) DO UPDATE SET v=excluded.v", {}},
             {"SELECT 3,length('ON CONFLICT RETURNING')", "ON CONFLICT(id) DO NOTHING", {{"3", "21"}}},
             {"SELECT s.id,s.v FROM source_rows s JOIN source_rows p ON s.id=p.id WHERE TRUE", "ON CONFLICT DO NOTHING", {{"2", "30"}}},
             {"SELECT 1,99", "ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE FALSE", {}},
             {"SELECT id,v FROM source_rows ORDER BY id DESC", "ON CONFLICT(id) DO UPDATE SET v=excluded.v", {{"2", "30"}, {"1", "20"}}},
             {"SELECT 2,NULL::INT", "ON CONFLICT(id) DO NOTHING", {{"2", "NULL"}}}}) {
        reset();
        const auto sql = "INSERT INTO target " + control.source + " " + control.action + " RETURNING id,v";
        check(run(sql).empty(), "source executes through typed owner");
        check(result.available && result.rows == control.rows, "exact source/returning row order and NULL display");
        check(result.columnTypes == std::vector<std::string>{"integer", "integer"}, "exact result descriptor");
        check(result.commandTag == "INSERT 0 " + std::to_string(control.rows.size()), "correct affected count");
        check(!g_engine.inTransaction(), "implicit owner released");
        if (control.source == "SELECT 2,NULL::INT")
            check(result.nulls == std::vector<std::vector<bool>>{{false, true}}, "genuine source NULL retained");
    }
    reset();
    const std::string failing = "INSERT INTO target SELECT id,10/(id-2) FROM source_rows ORDER BY id ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v";
    check(run(failing) == "22012", "later source error is not a syntax fallback");
    check(!result.available, "no partial result after source error");
    check(!g_engine.inTransaction(), "source cursor error releases implicit owner");
    check(g_engine.query(database, "target", {}, {"id", "v"}) == std::vector<std::string>{"1 10 "}, "all pending source writes withheld");
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.insertRow(database, "target", {{"id", "9"}, {"v", "90"}}) == DBStatus::OK);
    const auto xid = g_engine.currentTxnId();
    check(run(failing) == "22012", "caller receives source runtime error");
    check(g_engine.inTransaction() && g_engine.currentTxnId() == xid, "caller transaction retained");
    check(g_engine.query(database, "target", {"=id 9"}, {"v"}) == std::vector<std::string>{"90 "}, "earlier caller write retained");
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(fixture);
    std::cout << "INSERT_SOURCE_OWNER_CHECKS " << checks << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
