#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

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

    const std::string testName = "rename_column_expression_dependency";
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    // Deliberately make the table, source column, and a function share a
    // name.  Only column-reference tokens may change during the rename.
    table.tablename = "abs";
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("abs", false, 4));

    dbms::Column defaulted = dbms::makeIntColumn("defaulted", false, 4);
    defaulted.defaultValue = "abs(abs) + length('abs')";
    table.append(defaulted);

    dbms::Column checked = dbms::makeIntColumn("checked", false, 4);
    checked.checkExpr = "abs > 0 AND length('abs') = 3";
    checked.checkConstraintName = "positive_abs";
    table.append(checked);

    dbms::Column stored = dbms::makeIntColumn("stored_value", false, 4);
    stored.generatedExpr = "abs * 2";
    stored.generatedKind = 's';
    table.append(stored);

    dbms::Column virtualColumn =
        dbms::makeIntColumn("virtual_value", true, 4);
    virtualColumn.generatedExpr = "abs + 10";
    virtualColumn.generatedKind = 'v';
    table.append(virtualColumn);

    dbms::CheckConstraint tableCheck;
    tableCheck.name = "qualified_abs_limit";
    tableCheck.expression = "abs.abs < 10";
    table.additionalCheckConstraints.push_back(tableCheck);

    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "abs",
                           {{"id", "1"}, {"abs", "2"},
                            {"checked", "1"}}) == dbms::DBStatus::OK);

    assert(g_engine.alterTableRenameColumn(
               database, "abs", "abs", "amount") == dbms::DBStatus::OK);

    const dbms::TableSchema renamed =
        g_engine.getTableSchema(database, "abs");
    assert(renamed.cols[1].dataName == "amount");
    assert(renamed.cols[2].defaultValue ==
           "abs(amount) + length('abs')");
    assert(renamed.cols[3].checkExpr ==
           "amount > 0 AND length('abs') = 3");
    assert(renamed.cols[4].generatedExpr == "amount * 2");
    assert(renamed.cols[5].generatedExpr == "amount + 10");
    assert(renamed.additionalCheckConstraints.size() == 1);
    assert(renamed.additionalCheckConstraints[0].expression ==
           "abs.amount < 10");

    // Every expression must remain usable for future rows after a restart.
    {
        dbms::StorageEngine restarted;
        const dbms::TableSchema persisted =
            restarted.getTableSchema(database, "abs");
        assert(persisted.cols[2].defaultValue ==
               "abs(amount) + length('abs')");
        assert(persisted.cols[3].checkExpr ==
               "amount > 0 AND length('abs') = 3");
        assert(persisted.cols[4].generatedExpr == "amount * 2");
        assert(persisted.cols[5].generatedExpr == "amount + 10");
        assert(persisted.additionalCheckConstraints[0].expression ==
               "abs.amount < 10");
    }

    assert(g_engine.insert(database, "abs",
                           {{"id", "2"}, {"amount", "4"},
                            {"checked", "1"}}) == dbms::DBStatus::OK);
    const auto generated = g_engine.query(
        database, "abs", {"=id 2"},
        {"defaulted", "stored_value", "virtual_value"});
    assert(generated.size() == 1);
    assert(trimRight(generated[0]) == "7 8 14");

    assert(g_engine.update(database, "abs", {{"amount", "5"}},
                           {"=id 2"}) == dbms::DBStatus::OK);
    const auto updated = g_engine.query(
        database, "abs", {"=id 2"},
        {"defaulted", "stored_value", "virtual_value"});
    assert(updated.size() == 1);
    assert(trimRight(updated[0]) == "7 10 15");
    assert(g_engine.update(database, "abs", {{"amount", "-1"}},
                           {"=id 2"}) == dbms::DBStatus::INVALID_VALUE);

    assert(g_engine.insert(database, "abs",
                           {{"id", "3"}, {"amount", "-1"},
                            {"checked", "1"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "abs",
                           {{"id", "4"}, {"amount", "10"},
                            {"checked", "1"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[RENAME COLUMN EXPRESSION] dependencies preserved OK"
              << std::endl;
    return 0;
}
