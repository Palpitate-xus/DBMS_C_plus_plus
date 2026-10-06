#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void checkSignedColumn(const std::string& database, const std::string& table,
                       int scale, const std::string& minimum,
                       const std::string& maximum,
                       const std::string& below,
                       const std::string& above) {
    dbms::TableSchema schema;
    schema.tablename = table;
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("value", false, scale));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.insert(database, table,
                           {{"id", "1"}, {"value", minimum}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, table,
                           {{"id", "2"}, {"value", maximum}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, table,
                           {{"id", "3"}, {"value", below}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, table,
                           {{"id", "4"}, {"value", above}}) ==
           dbms::DBStatus::INVALID_VALUE);

    const auto minimumRows =
        g_engine.query(database, table, {"=value " + minimum}, {"id"});
    const auto maximumRows =
        g_engine.query(database, table, {"=value " + maximum}, {"id"});
    assert(minimumRows.size() == 1);
    assert(maximumRows.size() == 1);
    assert(g_engine.query(database, table, {}, {"id"}).size() == 2);

    assert(g_engine.update(database, table, {{"value", above}}, {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, table, {{"value", below}}, {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, table, {"=value " + minimum}, {"id"})
               .size() == 1);
}

void checkUnsignedColumn(const std::string& database, const std::string& table,
                         int scale, const std::string& maximum,
                         const std::string& above) {
    dbms::TableSchema schema;
    schema.tablename = table;
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("value", false, scale, false, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    const dbms::TableSchema stored = g_engine.getTableSchema(database, table);
    assert(stored.cols[1].isUnsigned);

    assert(g_engine.insert(database, table,
                           {{"id", "1"}, {"value", maximum}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, table,
                           {{"id", "2"}, {"value", "-1"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, table,
                           {{"id", "3"}, {"value", above}}) ==
           dbms::DBStatus::INVALID_VALUE);

    const auto rows = g_engine.query(database, table, {}, {"value"});
    assert(rows.size() == 1);
    assert(rows.front().find(maximum) != std::string::npos);
    assert(g_engine.query(database, table, {"=value " + maximum}, {"id"})
               .size() == 1);
    assert(g_engine.update(database, table, {{"value", "-1"}}, {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, table, {{"value", above}}, {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, table, {"=value " + maximum}, {"id"})
               .size() == 1);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "integer_bounds";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    checkUnsignedColumn(database, "utiny_values", 1, "255", "256");
    checkSignedColumn(database, "tiny_values", 1, "-128", "127", "-129",
                      "128");
    checkSignedColumn(database, "small_values", 0, "-32768", "32767",
                      "-32769", "32768");
    checkSignedColumn(database, "integer_values", 2, "-2147483648",
                      "2147483647", "-2147483649", "2147483648");
    checkSignedColumn(database, "bigint_values", 4,
                      "-9223372036854775807", "9223372036854775807",
                      "-9223372036854775809", "9223372036854775808");

    checkUnsignedColumn(database, "usmall_values", 0, "65535", "65536");
    checkUnsignedColumn(database, "uinteger_values", 2, "4294967295",
                        "4294967296");
    checkUnsignedColumn(database, "ubigint_values", 4,
                        "9223372036854775807", "9223372036854775808");

    dbms::StorageEngine::SelectExpr overflowDivision;
    overflowDivision.displayName = "result";
    overflowDivision.isScalar = true;
    overflowDivision.funcName = "arith";
    overflowDivision.funcArgs = {
        "-9223372036854775808", "/", "-1"};
    bool preciseOverflow = false;
    try {
        (void)g_engine.queryExpr(database, "utiny_values", {},
                                 {overflowDivision});
    } catch (const dbms::DbError& error) {
        preciseOverflow = error.sqlState() == "22003";
    }
    assert(preciseOverflow);

    dbms::StorageEngine::SelectExpr minimumModulo = overflowDivision;
    minimumModulo.funcArgs[1] = "%";
    assert(g_engine.queryExpr(database, "utiny_values", {},
                              {minimumModulo}) ==
           std::vector<std::string>{"0 "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[INTEGER BOUNDS] overflow and width checks OK" << std::endl;
    return 0;
}
