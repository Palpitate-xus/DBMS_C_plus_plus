#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "create_table_primary_key_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql(
        "CREATE TABLE duplicate_columns (id INT, id TEXT)", session));
    assert(!g_engine.tableExists(database, "duplicate_columns"));

    dbms::TableSchema directDuplicate;
    directDuplicate.tablename = "direct_duplicate_columns";
    directDuplicate.append(dbms::makeIntColumn("id", true, 2));
    directDuplicate.append(dbms::makeIntColumn("id", true, 2));
    std::string directError;
    assert(g_engine.createTable(
               database, directDuplicate, &directError) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(directError.find("duplicate column") != std::string::npos);
    assert(!g_engine.tableExists(database, directDuplicate.tablename));

    const auto directPrimaryKey = [](const std::string& name) {
        dbms::TableSchema table;
        table.tablename = name;
        table.append(dbms::makeIntColumn("id", false, 2));
        table.append(dbms::makeIntColumn("tenant", false, 2));
        return table;
    };

    dbms::TableSchema outOfRangePrimaryKey =
        directPrimaryKey("direct_out_of_range_primary_key");
    outOfRangePrimaryKey.pkColIndices = {2};
    assert(g_engine.createTable(database, outOfRangePrimaryKey) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, outOfRangePrimaryKey.tablename));

    dbms::TableSchema duplicatePrimaryKey =
        directPrimaryKey("direct_duplicate_primary_key");
    duplicatePrimaryKey.pkColIndices = {0, 0};
    assert(g_engine.createTable(database, duplicatePrimaryKey) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, duplicatePrimaryKey.tablename));

    dbms::TableSchema nullablePrimaryKey =
        directPrimaryKey("direct_nullable_primary_key");
    nullablePrimaryKey.cols[0].isNull = true;
    nullablePrimaryKey.pkColIndices = {0};
    assert(g_engine.createTable(database, nullablePrimaryKey) ==
           dbms::DBStatus::OK);
    const dbms::TableSchema normalizedPrimaryKey =
        g_engine.getTableSchema(database, nullablePrimaryKey.tablename);
    assert(!normalizedPrimaryKey.cols[0].isNull);
    assert(g_engine.insert(
               database, nullablePrimaryKey.tablename,
               {{"id", "NULL"}, {"tenant", "7"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);

    dbms::TableSchema conflictingPrimaryKey =
        directPrimaryKey("direct_conflicting_primary_key");
    conflictingPrimaryKey.pkColIndices = {0};
    conflictingPrimaryKey.cols[1].isPrimaryKey = true;
    assert(g_engine.createTable(database, conflictingPrimaryKey) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, conflictingPrimaryKey.tablename));

    dbms::TableSchema authoritativePrimaryKey =
        directPrimaryKey("direct_authoritative_primary_key");
    authoritativePrimaryKey.pkColIndices = {0, 1};
    assert(g_engine.createTable(database, authoritativePrimaryKey) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, authoritativePrimaryKey.tablename,
               {{"id", "1"}, {"tenant", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, authoritativePrimaryKey.tablename,
               {{"id", "1"}, {"tenant", "7"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    std::string maximumColumnsSql = "CREATE TABLE maximum_columns (";
    for (size_t i = 0; i < dbms::MAX_COLUMNS; ++i) {
        if (i != 0) maximumColumnsSql += ", ";
        maximumColumnsSql += "c" + std::to_string(i) + " INT";
    }
    maximumColumnsSql += ")";
    assert(!ddl.executeSql(maximumColumnsSql, session));
    assert(g_engine.getTableSchema(database, "maximum_columns").len ==
           dbms::MAX_COLUMNS);

    std::string tooManyColumnsSql = "CREATE TABLE too_many_columns (";
    for (size_t i = 0; i <= dbms::MAX_COLUMNS; ++i) {
        if (i != 0) tooManyColumnsSql += ", ";
        tooManyColumnsSql += "c" + std::to_string(i) + " INT";
    }
    tooManyColumnsSql += ")";
    assert(ddl.executeSql(tooManyColumnsSql, session));
    assert(!g_engine.tableExists(database, "too_many_columns"));
    assert(ddl.executeSql(
        "CREATE TABLE like_overflow (LIKE maximum_columns, extra INT)",
        session));
    assert(!g_engine.tableExists(database, "like_overflow"));

    assert(ddl.executeSql(
        "CREATE TABLE missing_pk (id INT, PRIMARY KEY (missing))",
        session));
    assert(!g_engine.tableExists(database, "missing_pk"));

    assert(ddl.executeSql(
        "CREATE TABLE duplicate_pk_column (id INT, PRIMARY KEY (id, id))",
        session));
    assert(!g_engine.tableExists(database, "duplicate_pk_column"));

    assert(ddl.executeSql(
        "CREATE TABLE multiple_pk (id INT PRIMARY KEY, code INT, "
        "PRIMARY KEY (code))",
        session));
    assert(!g_engine.tableExists(database, "multiple_pk"));

    assert(!ddl.executeSql(
        "CREATE TABLE valid_pk (id INT NULL, payload TEXT, PRIMARY KEY (id))",
        session));
    const dbms::TableSchema schema =
        g_engine.getTableSchema(database, "valid_pk");
    assert(schema.len == 2);
    assert(schema.cols[0].isPrimaryKey);
    assert(!schema.cols[0].isNull);
    assert(g_engine.insert(
               database, "valid_pk", {{"id", "NULL"}, {"payload", "x"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);

    cleanupTestDb(testName);
    std::cout << "[CREATE TABLE PK] definitions validated OK" << std::endl;
    return 0;
}
