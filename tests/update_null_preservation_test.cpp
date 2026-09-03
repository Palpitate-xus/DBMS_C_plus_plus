#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

int64_t primaryKeyRid(const std::string& database) {
    dbms::BPTree* index = g_engine.getPKIndex(database, "items");
    assert(index != nullptr);
    int64_t rid = -1;
    assert(index->search("1", rid));
    return rid;
}

void assertNullState(const std::string& database, bool noteIsNull,
                     bool countIsNull) {
    const int64_t rid = primaryKeyRid(database);
    assert(g_engine.isColumnNullByRid(database, "items", rid, 2) ==
           noteIsNull);
    assert(g_engine.isColumnNullByRid(database, "items", rid, 3) ==
           countIsNull);
    assert(!g_engine.isColumnNullByRid(database, "items", rid, 4));
    assert(g_engine.query(database, "items",
                          {std::string(noteIsNull ? "isnull " : "isnotnull ") +
                           "note"},
                          {"id"})
               .size() == 1);
    assert(g_engine.query(database, "items",
                          {std::string(countIsNull ? "isnull " : "isnotnull ") +
                           "optional_count"},
                          {"id"})
               .size() == 1);
    assert(g_engine.query(database, "items", {"isnotnull empty_text"},
                          {"id"})
               .size() == 1);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "update_null_preservation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("payload", false, 32));
    schema.append(dbms::makeVarCharColumn("note", true, 32));
    schema.append(dbms::makeIntColumn("optional_count", true, 4));
    schema.append(dbms::makeVarCharColumn("empty_text", true, 32));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    // Omitting note stores SQL NULL, while supplying an empty value stores a
    // real empty string. UPDATE must preserve that distinction.
    assert(g_engine.insert(database, "items",
                           {{"id", "1"}, {"payload", "before"},
                            {"empty_text", ""}}) == dbms::DBStatus::OK);
    assertNullState(database, true, true);

    assert(g_engine.update(database, "items", {{"payload", "after"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assertNullState(database, true, true);
    const auto rows =
        g_engine.query(database, "items", {"=id 1"}, {"payload"});
    assert(rows.size() == 1 && rows.front() == "after ");

    // An explicit empty string is not SQL NULL.
    assert(g_engine.update(database, "items", {{"note", ""}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assertNullState(database, false, true);

    // The explicit NULL marker creates a physical NULL for variable- and
    // fixed-width columns.
    assert(g_engine.update(database, "items",
                           {{"note", "NULL"}, {"optional_count", "7"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assertNullState(database, true, false);
    assert(g_engine.query(database, "items", {"=optional_count 7"}, {"id"})
               .size() == 1);
    assert(g_engine.update(database, "items", {{"optional_count", "NULL"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assertNullState(database, true, true);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[UPDATE NULL PRESERVATION] null bitmap retained OK"
              << std::endl;
    return 0;
}
