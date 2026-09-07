#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectDistinct(const std::string& database,
                            const std::string& function,
                            const std::string& left,
                            const std::string& right) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {left, right};

    const auto rows = g_engine.queryExpr(
        database, "distinct_projection_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectDistinct(const std::string& database,
                    const std::string& left,
                    const std::string& right,
                    bool distinct) {
    assert(projectDistinct(database, "isdistinct", left, right) ==
           (distinct ? "t" : "f"));
    assert(projectDistinct(database, "isnotdistinct", left, right) ==
           (distinct ? "f" : "t"));
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "distinct_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "distinct_projection_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeIntColumn("big_a", false, 3));
    schema.append(dbms::makeIntColumn("big_b", false, 3));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["empty_text"] = "";
    row["literal_null"] = "NULL";
    row["null_text"] = std::nullopt;
    row["big_a"] = "9007199254740992";
    row["big_b"] = "9007199254740993";
    assert(g_engine.insertRow(database, "distinct_projection_values", row) ==
           dbms::DBStatus::OK);

    expectDistinct(database, "empty_text", "NULL", true);
    expectDistinct(database, "literal_null", "NULL", true);
    expectDistinct(database, "null_text", "NULL", false);
    expectDistinct(database, "''", "NULL", true);
    expectDistinct(database, "'NULL'", "NULL", true);
    expectDistinct(database, "upper('null')", "NULL", true);
    expectDistinct(database, "upper(null_text)", "NULL", false);
    expectDistinct(database, "big_a", "big_b", true);
    expectDistinct(database, "big_a", "9007199254740992::bigint", false);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[DISTINCT PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
