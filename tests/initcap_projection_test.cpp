#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectInitcap(const std::string& database,
                           const std::string& argument) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "initcap";
    expression.isScalar = true;
    expression.funcName = "initcap";
    expression.funcArgs = {argument};

    const auto rows = g_engine.queryExpr(
        database, "initcap_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "initcap_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "initcap_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("phrase", false));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["phrase"] = "hI tHERE";
    row["empty_text"] = "";
    row["literal_null"] = "NULL";
    row["null_text"] = std::nullopt;
    assert(g_engine.insertRow(database, "initcap_values", row) ==
           dbms::DBStatus::OK);

    assert(projectInitcap(database, "phrase") == "Hi There");
    assert(projectInitcap(database, "'mIxEd-case 42TEST'") ==
           "Mixed-Case 42test");
    assert(projectInitcap(database, "empty_text").empty());
    assert(projectInitcap(database, "literal_null") == "Null");
    assert(projectInitcap(database, "null_text") == "NULL");
    assert(projectInitcap(database, "upper(null_text)") == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[INITCAP PROJECTION] all passed" << std::endl;
    return 0;
}
