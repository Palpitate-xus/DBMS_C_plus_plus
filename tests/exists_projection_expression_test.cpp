#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectExists(const std::string& database,
                          const std::string& subquery,
                          bool negate = false) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "?column?";
    expression.isScalar = true;
    expression.funcName = "exists_sub";
    expression.funcArgs = negate
        ? std::vector<std::string>{subquery, "not"}
        : std::vector<std::string>{subquery};

    const auto rows = g_engine.queryExpr(
        database, "exists_outer", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "exists_projection_expression";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema outer;
    outer.tablename = "exists_outer";
    outer.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    outer.append(dbms::makeIntColumn("id", false, 4, true));
    outer.append(dbms::makeTextColumn("tag", false));
    assert(g_engine.createTable(database, outer) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "exists_outer",
                           {{"id", "2"}, {"tag", "outer"}}) ==
           dbms::DBStatus::OK);

    dbms::TableSchema inner;
    inner.tablename = "exists_inner";
    inner.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    inner.append(dbms::makeIntColumn("x", false, 4, true));
    inner.append(dbms::makeTextColumn("tag", false));
    assert(g_engine.createTable(database, inner) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "exists_inner",
                           {{"x", "1"}, {"tag", "one and only"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "exists_inner",
                           {{"x", "2"}, {"tag", "two"}}) ==
           dbms::DBStatus::OK);

    assert(projectExists(
               database, "select 1 from exists_inner where false") == "f");
    assert(projectExists(
               database,
               "select 1 from exists_inner where x = 1 or x = 2") == "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner where x + 1 = 3") == "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner where x is null") == "f");
    assert(projectExists(
               database,
               "select 1 from exists_inner where "
               "exists_inner.x = exists_outer.id") == "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner where tag = 'two' and x = 2") ==
           "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner where tag = 'one and only'") ==
           "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner i where i.x = 2") == "t");
    assert(projectExists(
               database,
               "select 1 from exists_inner where x = 2", true) == "f");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[EXISTS PROJECTION EXPRESSION] all passed" << std::endl;
    return 0;
}
