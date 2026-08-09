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

    dbms::TableSchema source = table;
    source.tablename = "src";
    dbms::TableSchema copy = table;
    copy.tablename = "copy";
    assert(g_engine.createTable(db, source) == dbms::DBStatus::OK);
    assert(g_engine.createTable(db, copy) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "1"}, {"name", "one"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "2"}, {"name", "two"}}) == dbms::DBStatus::OK);

    dbms::DmlResult result;
    dbms::SQLParser parser;
    auto parsedInsertSelect = parser.parse(
        "INSERT INTO copy (id, name) SELECT id, name FROM src WHERE id >= 2 RETURNING id, name");
    assert(parsedInsertSelect.success);
    auto* parsedInsert = dynamic_cast<dbms::InsertStmt*>(parsedInsertSelect.stmt.get());
    assert(parsedInsert && parsedInsert->selectSource);
    auto* parsedSelect = dynamic_cast<dbms::SelectStmt*>(parsedInsert->selectSource.get());
    assert(parsedSelect && parsedSelect->whereClause);
    assert(!runDml("INSERT INTO copy (id, name) SELECT id, name FROM src "
                   "WHERE id >= 2 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"2", "two"}));

    assert(!runDml("INSERT INTO copy (id, name) SELECT id + 10, name FROM src "
                   "WHERE id = 1 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"11", "one"}));

    dbms::TableSchema conflict = table;
    conflict.tablename = "conflict_t";
    conflict.cols[0].isPrimaryKey = true;
    assert(g_engine.createTable(db, conflict) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "conflict_t", {{"id", "1"}, {"name", "old"}}) == dbms::DBStatus::OK);
    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'ignored'), (2, 'new') "
                   "ON CONFLICT DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert(result.rows.size() == 1);
    assert((result.rows[0] == std::vector<std::string>{"2", "new"}));
    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'ignored-again') "
                   "ON CONFLICT DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 0");
    assert(result.rows.empty());

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'updated'), (3, 'third') "
                   "ON CONFLICT (id) DO UPDATE SET name = 'upserted' "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "upserted"}, {"3", "third"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'excluded-update'), (4, 'four') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "excluded-update"}, {"4", "four"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'expression'), (5, 'five') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name || '-x' "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "expression-x"}, {"5", "five"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'where-update'), (6, 'six') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "WHERE name = 'expression-x' RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "where-update"}, {"6", "six"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'skipped'), (7, 'seven') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "WHERE name = 'does-not-match' RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"7", "seven"}}));

    assert(!runDml("INSERT INTO ret VALUES (3, 'inserted') RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"3", "inserted"}));
    assert(!runDml("INSERT INTO ret VALUES (4, 'expr') "
                   "RETURNING id + 10 AS next_id, name || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.columns == std::vector<std::string>{"next_id", "tagged"}));
    assert((result.rows[0] == std::vector<std::string>{"14", "expr-x"}));

    // The predicate column changes. A post-update query using the old WHERE
    // clause would return nothing; storage-boundary capture must return id=2.
    assert(!runDml("UPDATE ret SET id = 2, name = 'after' WHERE id = 1 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert(result.rows.size() == 1);
    assert((result.rows[0] == std::vector<std::string>{"2", "after"}));

    assert(!runDml("UPDATE ret SET name = 'after2' WHERE id = 2 "
                   "RETURNING id + 10 AS next_id, name || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows[0] == std::vector<std::string>{"12", "after2-x"}));

    assert(!runDml("DELETE FROM ret WHERE id = 2 RETURNING *", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert((result.rows[0] == std::vector<std::string>{"2", "after2"}));
    assert(!runDml("DELETE FROM ret WHERE id = 3", session));
    assert(!runDml("DELETE FROM ret WHERE id = 4 "
                   "RETURNING id * 2 AS doubled, name || '-deleted' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 1");
    assert((result.rows[0] == std::vector<std::string>{"8", "expr-deleted"}));
    assert(g_engine.query(db, "ret", {}, {}, {}).empty());

    cleanup(db);
    std::cout << "[DML-RETURNING] INSERT SELECT and storage-boundary RETURNING OK" << std::endl;
    return 0;
}
