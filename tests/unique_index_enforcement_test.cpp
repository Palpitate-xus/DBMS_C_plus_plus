#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void setupSession(Session& session, const std::string& database) {
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
}

std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

bool hasUniqueColumnIndex(dbms::StorageEngine& engine,
                          const std::string& database,
                          const std::string& table,
                          const std::string& column) {
    for (const auto& metadata :
         engine.getIndexMetadata(database, table)) {
        if (!metadata.isExpression && metadata.name == column) {
            return metadata.isUnique;
        }
    }
    return false;
}

void test_existing_duplicates_block_creation(dbms::DdlExecutor& ddl,
                                             Session& session,
                                             const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE existing_dups ("
        "id INT PRIMARY KEY, v VARCHAR(30) COLLATE nocase)", session));
    assert(g_engine.insert(
               database, "existing_dups",
               {{"id", "1"}, {"v", "Hello"}}) == DBStatus::OK);
    assert(g_engine.insert(
               database, "existing_dups",
               {{"id", "2"}, {"v", "HELLO"}}) == DBStatus::OK);

    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX existing_dups_v_uidx "
        "ON existing_dups(v)", session));
    assert(!g_engine.getNamedIndex(
                database, "existing_dups",
                "existing_dups_v_uidx").has_value());
    assert(!hasUniqueColumnIndex(
        g_engine, database, "existing_dups", "v"));

    // A failed build must not leave a hidden constraint behind.
    assert(g_engine.insert(
               database, "existing_dups",
               {{"id", "3"}, {"v", "heLLo"}}) == DBStatus::OK);
}

void test_single_column_lifecycle(dbms::DdlExecutor& ddl,
                                  Session& session,
                                  const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_single ("
        "id INT PRIMARY KEY, v VARCHAR(30) COLLATE app.casefold, "
        "note VARCHAR(30))", session));
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "1"}, {"v", "Alpha"}, {"note", "first"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "2"}, {"v", "Bravo"}, {"note", "second"}}) ==
           DBStatus::OK);
    for (const char* id : {"3", "4"}) {
        assert(g_engine.insert(
                   database, "unique_single",
                   {{"id", id}, {"v", "NULL"}, {"note", "nullable"}}) ==
               DBStatus::OK);
    }

    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_single_v_uidx "
        "ON unique_single(v) INCLUDE(note)", session));
    assert(hasUniqueColumnIndex(
        g_engine, database, "unique_single", "v"));
    assert(readBytes(std::filesystem::path(database) /
                     "unique_single.secidx")
               .rfind("DBMS_UNIQUE_INDEX_V1:v", 0) == 0);

    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "5"}, {"v", "aLpHa"}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "5"}, {"v", ""}}) == DBStatus::OK);
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "6"}, {"v", ""}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.update(
               database, "unique_single", {{"v", "ALPHA"}},
               {"=id 2"}) == DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(
               database, "unique_single", {"=v bravo"}, {"id"}).size() ==
           1);

    dbms::StorageEngine reopened;
    assert(hasUniqueColumnIndex(
        reopened, database, "unique_single", "v"));
    assert(reopened.insert(
               database, "unique_single",
               {{"id", "6"}, {"v", "ALpha"}}) ==
           DBStatus::DUPLICATE_KEY);

    assert(g_engine.reindex(database, "unique_single") == DBStatus::OK);
    assert(!ddl.executeSql(
        "ALTER TABLE unique_single RENAME COLUMN v TO renamed_v", session));
    assert(hasUniqueColumnIndex(
        g_engine, database, "unique_single", "renamed_v"));
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "6"}, {"renamed_v", "ALpha"}}) ==
           DBStatus::DUPLICATE_KEY);

    assert(!ddl.executeSql("DROP INDEX unique_single_v_uidx", session));
    assert(!hasUniqueColumnIndex(
        g_engine, database, "unique_single", "renamed_v"));
    assert(g_engine.insert(
               database, "unique_single",
               {{"id", "6"}, {"renamed_v", "alpha"}}) == DBStatus::OK);
}

void test_floating_equality(dbms::DdlExecutor& ddl,
                            Session& session,
                            const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_floats ("
        "id INT PRIMARY KEY, v DOUBLE PRECISION)",
        session));
    assert(g_engine.insert(
               database, "unique_floats",
               {{"id", "1"}, {"v", "-0"}}) == DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_floats_uidx ON unique_floats(v)",
        session));
    assert(g_engine.insert(
               database, "unique_floats",
               {{"id", "2"}, {"v", "0"}}) == DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "unique_floats",
               {{"id", "2"}, {"v", "NaN"}}) == DBStatus::OK);
    assert(g_engine.insert(
               database, "unique_floats",
               {{"id", "3"}, {"v", "nan"}}) == DBStatus::DUPLICATE_KEY);

    assert(!ddl.executeSql(
        "CREATE TABLE unique_float_dups ("
        "id INT PRIMARY KEY, v DOUBLE PRECISION)",
        session));
    assert(g_engine.insert(
               database, "unique_float_dups",
               {{"id", "1"}, {"v", "-0"}}) == DBStatus::OK);
    assert(g_engine.insert(
               database, "unique_float_dups",
               {{"id", "2"}, {"v", "0"}}) == DBStatus::OK);
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_float_dups_uidx "
        "ON unique_float_dups(v)", session));
    assert(!g_engine.getNamedIndex(
                database, "unique_float_dups",
                "unique_float_dups_uidx").has_value());
}

void test_composite_lifecycle(dbms::DdlExecutor& ddl,
                              Session& session,
                              const std::string& database) {
    using dbms::DBStatus;

    assert(!ddl.executeSql(
        "CREATE TABLE unique_composite ("
        "id INT PRIMARY KEY, tenant INT, "
        "v VARCHAR(30) COLLATE app.casefold)", session));
    assert(g_engine.insert(
               database, "unique_composite",
               {{"id", "1"}, {"tenant", "10"}, {"v", "Key"}}) ==
           DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE UNIQUE INDEX unique_composite_uidx "
        "ON unique_composite(tenant, v)", session));

    const auto indexes =
        g_engine.getCompositeIndexes(database, "unique_composite");
    assert(indexes.size() == 1);
    assert(indexes.front().name == "unique_composite_uidx");
    assert(indexes.front().isUnique);

    assert(g_engine.insert(
               database, "unique_composite",
               {{"id", "2"}, {"tenant", "10"}, {"v", "KEY"}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "unique_composite",
               {{"id", "2"}, {"tenant", "20"}, {"v", "KEY"}}) ==
           DBStatus::OK);
    for (const char* id : {"3", "4"}) {
        assert(g_engine.insert(
                   database, "unique_composite",
                   {{"id", id}, {"tenant", "10"}, {"v", "NULL"}}) ==
               DBStatus::OK);
    }
    assert(g_engine.update(
               database, "unique_composite", {{"tenant", "10"}},
               {"=id 2"}) == DBStatus::DUPLICATE_KEY);

    assert(!ddl.executeSql("DROP INDEX unique_composite_uidx", session));
    assert(g_engine.insert(
               database, "unique_composite",
               {{"id", "5"}, {"tenant", "10"}, {"v", "key"}}) ==
           DBStatus::OK);
}

void test_unsupported_unique_shapes_fail_closed(
    dbms::DdlExecutor& ddl, Session& session,
    const std::string& database) {
    assert(!ddl.executeSql(
        "CREATE TABLE unique_shapes (id INT, v VARCHAR(30))", session));

    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_partial ON unique_shapes(v) "
        "WHERE id > 0", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_collated ON unique_shapes(v COLLATE C)",
        session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_opclass "
        "ON unique_shapes(v text_pattern_ops)", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_hash "
        "ON unique_shapes USING HASH(v)", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX CONCURRENTLY unique_concurrent "
        "ON unique_shapes(v)", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_expression "
        "ON unique_shapes((UPPER(v)))", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_nulls_not_distinct "
        "ON unique_shapes(v) NULLS NOT DISTINCT", session));

    for (const char* name : {
             "unique_partial", "unique_collated", "unique_opclass",
             "unique_hash", "unique_concurrent", "unique_expression",
             "unique_nulls_not_distinct"}) {
        assert(!g_engine.getNamedIndex(
                    database, "unique_shapes", name).has_value());
    }
    assert(g_engine.getIndexMetadata(
               database, "unique_shapes").empty());

    assert(!ddl.executeSql(
        "CREATE TABLE unique_partitioned (id INT, v VARCHAR(30)) "
        "PARTITION BY HASH (id)", session));
    assert(ddl.executeSql(
        "CREATE UNIQUE INDEX unique_partitioned_uidx "
        "ON unique_partitioned(id, v)", session));
    assert(!g_engine.getNamedIndex(
                database, "unique_partitioned",
                "unique_partitioned_uidx").has_value());
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "unique_index_enforcement";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    setupSession(session, database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", session));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.casefold "
        "(provider=libc, locale='nocase')", session));

    test_existing_duplicates_block_creation(ddl, session, database);
    test_single_column_lifecycle(ddl, session, database);
    test_floating_equality(ddl, session, database);
    test_composite_lifecycle(ddl, session, database);
    test_unsupported_unique_shapes_fail_closed(ddl, session, database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[UNIQUE INDEX] all passed" << std::endl;
    return 0;
}
