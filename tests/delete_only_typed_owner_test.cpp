#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "delete_only_typed_owner", database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT,v TEXT)", session));
    assert(g_engine.insertRow(database, "target", {{"id", "1"}, {"v", "one"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "target", {{"id", "2"}, {"v", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, "target", {{"id", "3"}, {"v", "three"}}) == DBStatus::OK);
    const auto snapshot = [&] { return g_engine.query(database, "target", {}, {"id", "v"}, {{"id", true}}); };
    const auto initial = snapshot();
    const auto run = [&](const std::string& sql, const std::string& expected = std::string{}) {
        bool handled = false, failed = false;
        std::string state;
        clearLastDmlResult();
        try { failed = tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql); }
        catch (const DbError& error) { state = error.sqlState(); handled = true; }
        std::cout << "DELETE_ONLY_NATIVE " << sql << " handled=" << handled
                  << " failed=" << failed << " state=" << state << std::endl;
        assert(handled && !failed && state == expected && currentSession() == nullptr);
        return takeLastDmlResult();
    };
    auto result = run("DELETE FROM ONLY target AS t WHERE false RETURNING t.id,t.v");
    assert(result.available && result.commandTag == "DELETE 0" && result.rows.empty());
    assert(result.columnTypes == std::vector<std::string>({"integer", "text"}));
    assert(snapshot() == initial);
    (void)run("DELETE FROM ONLY target AS t WHERE target.id=2 RETURNING t.id", "42P01");
    assert(snapshot() == initial && !g_engine.inTransaction());
    (void)run("DELETE FROM ONLY target WHERE id<3 RETURNING 10/(id-2)", "22012");
    assert(snapshot() == initial && !g_engine.inTransaction());
    result = run("DELETE FROM ONLY (target) AS t WHERE t.id=2 RETURNING t.id,t.v");
    assert(result.commandTag == "DELETE 1" && result.rows == std::vector<std::vector<std::string>>({{"2", "NULL"}}));
    assert(result.nulls == std::vector<std::vector<bool>>({{false, true}}));
    assert(snapshot() == std::vector<std::string>({"1 one ", "3 three "}));
    for (const std::string sql : {"DELETE FROM ONLY (target WHERE id=1", "DELETE FROM ONLY (target,target)"}) {
        SQLParser parser;
        const auto parsed = parser.parseForBinding(sql);
        assert(!parsed.isValid() && parsed.sqlState == "42601");
    }
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[DELETE ONLY TYPED OWNER] direct bridge, NULL and atomic error controls passed\n";
}
