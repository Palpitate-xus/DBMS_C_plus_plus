#include "Session.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

bool runDml(const std::string& sql, Session& session) {
    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        sql, dbms::SQLParser::classify(sql), session, handled);
    assert(handled);
    return error;
}

dbms::TableSchema makeTable(const std::string& name, bool nullable) {
    dbms::TableSchema table;
    table.tablename = name;
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeVarCharColumn("value", nullable, 32));
    return table;
}

dbms::TableSchema makeUniqueTable() {
    dbms::TableSchema table = makeTable("unique_t", true);
    table.cols[1].isUnique = true;
    return table;
}

dbms::TableSchema makeTextKeyTable() {
    dbms::TableSchema table;
    table.tablename = "text_key_t";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeVarCharColumn("value", false, 32, true));
    return table;
}

void assertStoredStates(const std::string& database) {
    const auto table = g_engine.getTableSchema(database, "values_t");
    std::map<std::string, std::pair<std::string, bool>> rows;
    assert(g_engine.forEachRow(
        database, "values_t",
        [&](uint32_t pageId, uint16_t slotId, const char* data, size_t len) {
            const std::string row(data, len);
            const std::string id = g_engine.extractColumnValue(
                row, table, 0, database, true);
            const std::string value = g_engine.extractColumnValue(
                row, table, 1, database, true);
            const int64_t rid = dbms::StorageEngine::encodeRid(pageId, slotId);
            rows[id] = {value, g_engine.isColumnNullByRid(
                                   database, "values_t", rid, 1)};
        }));
    assert(rows.size() == 3);
    assert(rows.at("1") == std::make_pair(std::string("NULL"), false));
    assert(rows.at("2") == std::make_pair(std::string(), true));
    assert(rows.at("3") == std::make_pair(std::string(), false));
}

}  // namespace

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "dml_null_literal_boundary";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createTable(database, makeTable("values_t", true)) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTable(database, makeTable("required_t", false)) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTable(database, makeUniqueTable()) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTable(database, makeTextKeyTable()) ==
           dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;

    assert(!runDml(
        "INSERT INTO values_t VALUES (1, 'NULL'), (2, NULL), (3, '') "
        "RETURNING id, value, value IS NULL",
        session));
    dbms::DmlResult result = dbms::takeLastDmlResult();
    assert(result.available);
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "NULL", "f"}, {"2", "NULL", "t"}, {"3", "", "f"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{
        {false, false, false}, {false, true, false},
        {false, false, false}}));
    assertStoredStates(database);

    assert(!runDml("INSERT INTO required_t VALUES (1, 'NULL')", session));
    assert(runDml("INSERT INTO required_t VALUES (2, NULL)", session));

    // Index/constraint keys must treat the four-byte text as data. SQL NULLs
    // remain exempt from UNIQUE, while text 'NULL' conflicts with itself.
    assert(!runDml(
        "INSERT INTO unique_t VALUES (1, 'NULL'), (2, NULL), (3, NULL)",
        session));
    assert(runDml("INSERT INTO unique_t VALUES (4, 'NULL')", session));

    // Text 'NULL' and an empty string are distinct primary-key values.
    assert(!runDml(
        "INSERT INTO text_key_t VALUES ('NULL'), ('')", session));
    assert(runDml("INSERT INTO text_key_t VALUES ('NULL')", session));
    size_t textKeyRows = 0;
    assert(g_engine.forEachRow(
        database, "text_key_t",
        [&](uint32_t, uint16_t, const char*, size_t) { ++textKeyRows; }));
    assert(textKeyRows == 2);

    assert(!runDml(
        "UPDATE values_t SET value = NULL WHERE id = 1 "
        "RETURNING value, value IS NULL",
        session));
    result = dbms::takeLastDmlResult();
    assert((result.rows ==
            std::vector<std::vector<std::string>>{{"NULL", "t"}}));
    assert((result.nulls ==
            std::vector<std::vector<bool>>{{true, false}}));

    assert(!runDml(
        "UPDATE values_t SET value = upper('null') WHERE id = 2 "
        "RETURNING value, value IS NULL",
        session));
    result = dbms::takeLastDmlResult();
    assert((result.rows ==
            std::vector<std::vector<std::string>>{{"NULL", "f"}}));
    assert((result.nulls ==
            std::vector<std::vector<bool>>{{false, false}}));

    assert(!runDml(
        "DELETE FROM values_t WHERE id = 2 "
        "RETURNING value, value IS NULL",
        session));
    result = dbms::takeLastDmlResult();
    assert((result.rows ==
            std::vector<std::vector<std::string>>{{"NULL", "f"}}));
    assert((result.nulls ==
            std::vector<std::vector<bool>>{{false, false}}));

    assert(!runDml(
        "DELETE FROM values_t WHERE id = 1 "
        "RETURNING value, value IS NULL",
        session));
    result = dbms::takeLastDmlResult();
    assert((result.rows ==
            std::vector<std::vector<std::string>>{{"NULL", "t"}}));
    assert((result.nulls ==
            std::vector<std::vector<bool>>{{true, false}}));

    // Existing embedded callers retain their documented marker convention.
    assert(g_engine.insert(database, "values_t",
                           {{"id", "4"}, {"value", "NULL"}}) ==
           dbms::DBStatus::OK);
    dbms::BPTree* index = g_engine.getPKIndex(database, "values_t");
    assert(index != nullptr);
    int64_t legacyRid = -1;
    assert(index->search("4", legacyRid));
    assert(g_engine.isColumnNullByRid(
        database, "values_t", legacyRid, 1));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[DML NULL LITERAL BOUNDARY] all passed" << std::endl;
    return 0;
}
