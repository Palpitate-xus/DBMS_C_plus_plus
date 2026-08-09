#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) {
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
}

static bool runDml(const std::string& sql, Session& session) {
    bool handled = false;
    const auto command = dbms::SQLParser::classify(sql);
    const bool error = dbms::tryDmlBridge(sql, command, session, handled);
    assert(handled);
    return error;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("dml_returning");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;

    dbms::TableSchema table;
    table.tablename = "ret";
    dbms::Column id;
    id.dataName = "id";
    id.dataType = "integer";
    id.dsize = 4;
    table.append(id);
    dbms::Column name;
    name.dataName = "name";
    name.dataType = "varchar";
    name.isVariableLength = true;
    name.dsize = 32;
    table.append(name);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "ret", {{"id", "1"}, {"name", "before"}}) == dbms::DBStatus::OK);

    assert(!runDml("INSERT INTO ret VALUES (3, 'inserted') RETURNING id, name", session));
    dbms::DmlResult result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"3", "inserted"}));

    // The predicate column changes. A post-update query using the old WHERE
    // clause would return nothing; storage-boundary capture must return id=2.
    assert(!runDml("UPDATE ret SET id = 2, name = 'after' WHERE id = 1 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert(result.rows.size() == 1);
    assert((result.rows[0] == std::vector<std::string>{"2", "after"}));

    assert(!runDml("DELETE FROM ret WHERE id = 2 RETURNING *", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert((result.rows[0] == std::vector<std::string>{"2", "after"}));
    assert(!runDml("DELETE FROM ret WHERE id = 3", session));
    assert(g_engine.query(db, "ret", {}, {}, {}).empty());

    cleanup(db);
    std::cout << "[DML-RETURNING] storage-boundary UPDATE/DELETE RETURNING OK" << std::endl;
    return 0;
}
