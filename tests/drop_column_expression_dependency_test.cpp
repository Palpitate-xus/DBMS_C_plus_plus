#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

size_t rowCount(const std::string& database, const std::string& tableName) {
    size_t count = 0;
    assert(g_engine.forEachRow(
        database, tableName,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

std::string trimRight(std::string value) {
    const size_t end = value.find_last_not_of(" \t\r\n");
    if (end == std::string::npos) return {};
    value.resize(end + 1);
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "drop_column_expression_dependency";
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE generated_values ("
        "id INT PRIMARY KEY, base INT, spare INT, "
        "computed INT GENERATED ALWAYS AS (base + 1) STORED)",
        session));
    assert(g_engine.insert(database, "generated_values",
                           {{"id", "1"}, {"base", "5"}, {"spare", "9"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableDropColumn(
               database, "generated_values", "base") ==
           dbms::DBStatus::INVALID_VALUE);
    dbms::TableSchema generated =
        g_engine.getTableSchema(database, "generated_values");
    assert(generated.len == 4);
    assert(generated.cols[1].dataName == "base");
    assert(rowCount(database, "generated_values") == 1);

    assert(g_engine.alterTableDropColumn(
               database, "generated_values", "spare") ==
           dbms::DBStatus::OK);
    generated = g_engine.getTableSchema(database, "generated_values");
    assert(generated.len == 3);
    assert(rowCount(database, "generated_values") == 1);
    assert(g_engine.insert(database, "generated_values",
                           {{"id", "2"}, {"base", "10"}}) ==
           dbms::DBStatus::OK);
    const auto generatedRows =
        g_engine.query(database, "generated_values", {"=id 2"}, {"computed"});
    assert(generatedRows.size() == 1);
    assert(trimRight(generatedRows[0]) == "11");

    assert(!ddl.executeSql(
        "CREATE TABLE checked_values ("
        "id INT PRIMARY KEY, checked INT, spare INT, "
        "CONSTRAINT id_positive CHECK (id > 0), "
        "CONSTRAINT checked_positive CHECK (checked > 0))",
        session));
    const dbms::TableSchema initialChecked =
        g_engine.getTableSchema(database, "checked_values");
    assert(initialChecked.additionalCheckConstraints.size() == 1);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "1"}, {"checked", "5"}, {"spare", "9"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableDropColumn(
               database, "checked_values", "checked") ==
           dbms::DBStatus::INVALID_VALUE);
    dbms::TableSchema checked =
        g_engine.getTableSchema(database, "checked_values");
    assert(checked.len == 3);
    assert(checked.cols[1].dataName == "checked");
    assert(rowCount(database, "checked_values") == 1);

    assert(g_engine.alterTableDropColumn(
               database, "checked_values", "spare") == dbms::DBStatus::OK);
    assert(rowCount(database, "checked_values") == 1);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "2"}, {"checked", "-1"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    // Identifier text inside a literal is not a column dependency.
    assert(!ddl.executeSql(
        "CREATE TABLE literal_check ("
        "id INT PRIMARY KEY, marker INT, "
        "CONSTRAINT literal_nonempty CHECK (length('marker') > 0))",
        session));
    assert(g_engine.insert(database, "literal_check",
                           {{"id", "1"}, {"marker", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableDropColumn(
               database, "literal_check", "marker") ==
           dbms::DBStatus::OK);
    assert(rowCount(database, "literal_check") == 1);

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[DROP COLUMN EXPRESSION] dependencies preserved OK"
              << std::endl;
    return 0;
}
