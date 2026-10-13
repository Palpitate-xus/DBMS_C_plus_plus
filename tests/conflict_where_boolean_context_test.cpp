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
    const std::string fixture = "conflict_where_boolean_context", database = testDbPath(fixture);
    cleanupTestDb(fixture);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "target";
    table.append(makeIntColumn("id", false, 2, true));
    table.append(makeIntColumn("v", true, 2));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    Session session;
    session.permission = 1;
    session.username = "admin";
    session.currentDB = database;
    size_t checks = 0, failures = 0;
    DmlResult result;
    const auto check = [&](bool good, const std::string& name) {
        ++checks;
        if (!good) { ++failures; std::cerr << "CONFLICT_BOOLEAN_NATIVE_FAILURE " << name << '\n'; }
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
        std::cout << "CONFLICT_BOOLEAN_NATIVE " << sql << " STATE " << state << std::endl;
        return state;
    };
    const auto reset = [&] {
        assert(!g_engine.inTransaction());
        assert(g_engine.remove(database, "target", {}) == DBStatus::OK);
        assert(g_engine.insertRow(database, "target", {{"id", "1"}, {"v", "10"}}) == DBStatus::OK);
    };
    const auto sql = [](const std::string& source, const std::string& condition) {
        return "INSERT INTO target " + source + " ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE " + condition + " RETURNING id,v";
    };
    for (const auto& bad : std::vector<std::pair<std::string, std::string>>{
             {"1", "42804"}, {"NULL::INT", "42804"}, {"'notboolean'", "22P02"},
             {"'f'::TEXT", "42804"}, {"NULL::TEXT", "42804"}}) {
        for (const auto& source : {"VALUES(1,99)", "VALUES(5,99)", "SELECT 5,99 WHERE FALSE"}) {
            reset();
            check(run(sql(source, bad.first)) == bad.second, "pre-execution Boolean error");
            check(!g_engine.inTransaction(), "no leaked implicit transaction");
            check(!result.available, "no failed RETURNING publication");
            check(g_engine.query(database, "target", {}, {"id", "v"}) == std::vector<std::string>{"1 10 "}, "original rows retained");
        }
    }
    for (const auto& good : std::vector<std::pair<std::string, bool>>{
             {"'f'", false}, {"'off'", false}, {"FALSE", false}, {"NULL", false},
             {"NULL::BOOL", false}, {"'t'", true}, {"'yes'", true}, {"TRUE", true},
             {"excluded.v>target.v", true}, {"(SELECT excluded.v>target.v)", true}, {"(SELECT FALSE)", false}}) {
        reset();
        check(run(sql("VALUES(1,99)", good.first)).empty(), "accepted typed Boolean input");
        check(result.available && result.columnTypes == std::vector<std::string>{"integer", "integer"}, "exact RETURNING descriptor");
        check(result.rows == (good.second ? std::vector<std::vector<std::string>>{{"1", "99"}} : std::vector<std::vector<std::string>>{}), "exact returning rows");
        check(result.commandTag == (good.second ? "INSERT 0 1" : "INSERT 0 0"), "exact conflict affected count");
        check(g_engine.query(database, "target", {}, {"id", "v"}) == std::vector<std::string>{good.second ? "1 99 " : "1 10 "}, "stored row follows Boolean value");
    }
    reset();
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.insertRow(database, "target", {{"id", "2"}, {"v", "20"}}) == DBStatus::OK);
    const auto xid = g_engine.currentTxnId();
    check(run(sql("VALUES(1,999)", "1")) == "42804", "caller transaction rejection");
    check(g_engine.inTransaction() && g_engine.currentTxnId() == xid, "caller owner retained");
    check(g_engine.query(database, "target", {"=id 2"}, {"v"}) == std::vector<std::string>{"20 "}, "earlier caller work retained");
    check(g_engine.query(database, "target", {"=id 1"}, {"v"}) == std::vector<std::string>{"10 "}, "failed conflict unchanged");
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(fixture);
    std::cout << "CONFLICT_BOOLEAN_NATIVE_CHECKS " << checks << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
