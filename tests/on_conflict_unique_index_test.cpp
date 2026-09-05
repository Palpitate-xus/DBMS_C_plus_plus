#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

bool runDml(const std::string& sql, Session& session) {
    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        sql, dbms::SQLParser::classify(sql), session, handled, sql);
    assert(handled);
    return error;
}

void expectReturning(const std::string& commandTag,
                     const std::vector<std::vector<std::string>>& rows) {
    const dbms::DmlResult result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == commandTag);
    assert(result.rows == rows);
}

void test_single_column_index(dbms::DdlExecutor& ddl, Session& session,
                              const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_single ("
        "id INT PRIMARY KEY, key_value VARCHAR(30) COLLATE app.casefold, "
        "payload VARCHAR(30))",
        session));
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "1"}, {"key_value", "Alpha"},
                {"payload", "initial"}}) == DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_single_key_uidx "
        "ON unique_single(key_value) INCLUDE(payload)",
        session));

    // A standalone CREATE UNIQUE INDEX is a valid conflict arbiter. Its
    // column collation, rather than the physical B-tree byte order, defines
    // which existing row is updated.
    assert(!runDml(
        "INSERT INTO unique_single VALUES (2, 'aLPHa', 'changed') "
        "ON CONFLICT (key_value) DO UPDATE "
        "SET payload = excluded.payload "
        "RETURNING id, key_value, payload",
        session));
    expectReturning("INSERT 0 1", {{"1", "Alpha", "changed"}});
    assert(g_engine.query(
               database, "unique_single", {"=id 2"}, {"id"}).empty());

    // Empty text is an ordinary non-NULL unique key and remains visible to
    // both conflict-row lookup and target-row WHERE evaluation.
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "3"}, {"key_value", ""}, {"payload", ""}}) ==
           DBStatus::OK);
    assert(!runDml(
        "INSERT INTO unique_single VALUES (4, '', 'filled') "
        "ON CONFLICT (key_value) DO UPDATE "
        "SET payload = excluded.payload WHERE payload = '' "
        "RETURNING id, key_value, payload",
        session));
    expectReturning("INSERT 0 1", {{"3", "", "filled"}});

    // A target-specific action must not consume a duplicate raised solely by
    // another unique key (the primary key in this case).
    assert(runDml(
        "INSERT INTO unique_single VALUES (1, 'Zulu', 'wrong') "
        "ON CONFLICT (key_value) DO NOTHING",
        session));
    assert(g_engine.query(
               database, "unique_single", {"=key_value Zulu"}, {"id"})
               .empty());

    assert(!runDml(
        "INSERT INTO unique_single VALUES (2, 'ALPHA', 'ignored') "
        "ON CONFLICT (key_value) DO NOTHING RETURNING id",
        session));
    expectReturning("INSERT 0 0", {});

    // PostgreSQL-style unique indexes use NULLS DISTINCT by default.
    assert(!runDml(
        "INSERT INTO unique_single VALUES (5, NULL, 'null-one') "
        "ON CONFLICT (key_value) DO NOTHING",
        session));
    assert(!runDml(
        "INSERT INTO unique_single VALUES (6, NULL, 'null-two') "
        "ON CONFLICT (key_value) DO NOTHING",
        session));
    assert(g_engine.query(
               database, "unique_single", {}, {"id"}).size() == 4);
}

void test_floating_index(dbms::DdlExecutor& ddl, Session& session,
                         const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_float ("
        "id INT PRIMARY KEY, key_value DOUBLE PRECISION, payload VARCHAR(30))",
        session));
    assert(g_engine.insert(
               database, "unique_float",
               {{"id", "1"}, {"key_value", "-0"},
                {"payload", "negative-zero"}}) == DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_float_key_uidx "
        "ON unique_float(key_value)",
        session));

    assert(!runDml(
        "INSERT INTO unique_float VALUES (2, 0, 'canonical-zero') "
        "ON CONFLICT (key_value) DO UPDATE "
        "SET payload = excluded.payload RETURNING id, payload",
        session));
    expectReturning("INSERT 0 1", {{"1", "canonical-zero"}});
    assert(g_engine.query(
               database, "unique_float", {"=id 2"}, {"id"}).empty());
}

void test_composite_index(dbms::DdlExecutor& ddl, Session& session,
                          const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_composite ("
        "id INT PRIMARY KEY, tenant INT, "
        "key_value VARCHAR(30) COLLATE app.casefold, payload VARCHAR(30))",
        session));
    assert(g_engine.insert(
               database, "unique_composite",
               {{"id", "1"}, {"tenant", "10"}, {"key_value", "Bravo"},
                {"payload", "initial"}}) == DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_composite_key_uidx "
        "ON unique_composite(tenant, key_value)",
        session));

    // Inference is order-independent, while row matching still checks every
    // component with its SQL type and collation semantics.
    assert(!runDml(
        "INSERT INTO unique_composite VALUES (2, 10, 'bRAVO', 'changed') "
        "ON CONFLICT (key_value, tenant) DO UPDATE "
        "SET payload = excluded.payload RETURNING id, tenant, payload",
        session));
    expectReturning("INSERT 0 1", {{"1", "10", "changed"}});
    assert(g_engine.query(
               database, "unique_composite", {"=id 2"}, {"id"}).empty());
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "on_conflict_unique_index";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;

    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", session));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.casefold "
        "(provider=libc, locale='nocase')",
        session));

    test_single_column_index(ddl, session, database);
    test_floating_index(ddl, session, database);
    test_composite_index(ddl, session, database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ON CONFLICT UNIQUE INDEX] all passed" << std::endl;
    return 0;
}
