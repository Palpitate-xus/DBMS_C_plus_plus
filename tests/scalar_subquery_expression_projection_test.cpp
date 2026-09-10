#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "scalar_subquery_expression_projection";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema outer;
    outer.tablename = "outer_rows";
    outer.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    outer.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, outer) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, outer.tablename, {{"id", "1"}}) == dbms::DBStatus::OK);
    dbms::TableSchema inner = outer;
    inner.tablename = "inner_rows";
    inner.append(dbms::makeTextColumn("text_value", false));
    inner.append(dbms::makeTextColumn("empty_value", false));
    inner.append(dbms::makeTextColumn("null_value", true));
    inner.append(dbms::makeTextColumn("null_text", false));
    assert(g_engine.createTable(database, inner) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, inner.tablename,
                              {{"id", "9"}, {"text_value", " two words "},
                               {"empty_value", ""}, {"null_value", std::nullopt},
                               {"null_text", "NULL"}}) == dbms::DBStatus::OK);

    auto project = [&](const std::string& target) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = "subquery";
        expression.funcArgs = {"select " + target + " from inner_rows"};
        return g_engine.queryExpr(database, outer.tablename, {}, {expression});
    };
    auto projectStructured = [&](const std::string& target) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = "subquery";
        expression.funcArgs = {"select " + target + " from inner_rows"};
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        (void)g_engine.queryExpr(database, outer.tablename, {}, {expression},
                                 {}, &rows, &nulls);
        assert(rows.size() == 1 && rows.front().size() == 1);
        assert(nulls.size() == 1 && nulls.front().size() == 1);
        return std::make_pair(rows.front().front(),
                              static_cast<bool>(nulls.front().front()));
    };
    assert((project("1") == std::vector<std::string>{"1 "}));
    assert((project("id + 2") == std::vector<std::string>{"11 "}));
    assert((project("inner_rows.id") == std::vector<std::string>{"9 "}));
    assert((project("id AS value") == std::vector<std::string>{"9 "}));
    assert((project("text_value") == std::vector<std::string>{" two words  "}));
    assert((project("empty_value") == std::vector<std::string>{" "}));
    assert((project("empty_value IS NULL") == std::vector<std::string>{"f "}));
    assert((project("null_text IS NULL") == std::vector<std::string>{"f "}));
    assert((project("null_value IS NULL") == std::vector<std::string>{"t "}));
    assert((project("null_value") == std::vector<std::string>{"NULL "}));
    assert(projectStructured("null_text") ==
           std::make_pair(std::string("NULL"), false));
    assert(projectStructured("null_value") ==
           std::make_pair(std::string(), true));
    assert(projectStructured("empty_value") ==
           std::make_pair(std::string(), false));

    assert(g_engine.beginTransaction(database, false) == dbms::DBStatus::OK);
    assert(g_engine.update(database, inner.tablename, {{"id", "10"}}, {"=id 9"}) ==
           dbms::DBStatus::OK);
    assert((project("id + 2") == std::vector<std::string>{"12 "}));
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert((project("id + 2") == std::vector<std::string>{"11 "}));

    bool rejected = false;
    try {
        (void)project("1 / 0");
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find("division by zero") != std::string::npos;
    }
    assert(rejected);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY EXPRESSION PROJECTION] passed\n";
}
