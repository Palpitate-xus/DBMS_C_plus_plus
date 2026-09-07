#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectMath(const std::string& database,
                        const std::string& function,
                        const std::string& left,
                        const std::string& right) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {left, right};

    const auto rows = g_engine.queryExpr(
        database, "math_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "gcd_lcm_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "math_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("null_value", true, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "math_values",
                           {{"id", "1"}, {"null_value", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectMath(database, "gcd", "cast(10.50 as numeric)",
                       "cast(3.0 as numeric)") == "1.50");
    assert(projectMath(database, "lcm", "cast(10.50 as numeric)",
                       "cast(3.0 as numeric)") == "21.00");
    assert(projectMath(database, "gcd",
                       "cast(12345678901234567890.12 as numeric)",
                       "cast(0.04 as numeric)") == "0.04");
    assert(projectMath(database, "gcd", "54", "24") == "6");
    assert(projectMath(database, "lcm", "4", "6") == "12");
    assert(projectMath(database, "gcd", "null_value", "6") == "NULL");
    assert(projectMath(database, "gcd",
                       "cast('Infinity' as numeric)",
                       "cast(3 as numeric)") == "NaN");

    bool rejectedOverflow = false;
    try {
        (void)projectMath(database, "lcm", "9223372036854775807", "2");
    } catch (const std::runtime_error& error) {
        rejectedOverflow =
            std::string(error.what()).find("SQLSTATE 22003") !=
            std::string::npos;
    }
    assert(rejectedOverflow);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[GCD LCM PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
