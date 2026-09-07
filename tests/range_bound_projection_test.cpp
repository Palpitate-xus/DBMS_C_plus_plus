#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectBound(const std::string& database,
                         const std::string& function,
                         const std::string& argument) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {argument};

    const auto rows = g_engine.queryExpr(
        database, "range_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "range_bound_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "range_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeInt4RangeColumn("bounded", false));
    schema.append(dbms::makeInt4RangeColumn("lower_unbounded", false));
    schema.append(dbms::makeInt4RangeColumn("upper_unbounded", false));
    schema.append(dbms::makeInt4RangeColumn("empty_range", false));
    schema.append(dbms::makeInt4RangeColumn("null_range", true));
    schema.append(dbms::makeTextColumn("text_value", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "range_values",
                           {{"id", "1"},
                            {"bounded", "[1,10)"},
                            {"lower_unbounded", "(,10)"},
                            {"upper_unbounded", "[1,)"},
                            {"empty_range", "empty"},
                            {"null_range", "NULL"},
                            {"text_value", "HeLLo"}}) ==
           dbms::DBStatus::OK);

    assert(projectBound(database, "lower", "bounded") == "1");
    assert(projectBound(database, "upper", "bounded") == "10");
    assert(projectBound(database, "lower", "lower_unbounded") == "NULL");
    assert(projectBound(database, "upper", "upper_unbounded") == "NULL");
    assert(projectBound(database, "lower", "empty_range") == "NULL");
    assert(projectBound(database, "upper", "null_range") == "NULL");
    assert(projectBound(database, "lower",
                        "cast('[3,8)' as int4range)") == "3");
    assert(projectBound(database, "upper",
                        "cast('[3,8)' as int4range)") == "8");
    assert(projectBound(database, "lower", "text_value") == "hello");
    assert(projectBound(database, "upper", "text_value") == "HELLO");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[RANGE BOUND PROJECTION] all passed" << std::endl;
    return 0;
}
