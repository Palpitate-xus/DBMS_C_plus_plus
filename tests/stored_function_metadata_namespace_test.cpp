#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "stored_function_metadata_namespace";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.len = 1;
    table.cols[0].dataName = "id";
    table.cols[0].dataType = "int";
    table.cols[0].dsize = 4;
    assert(g_engine.createTable(db, "metadata_calls", table) == DBStatus::OK);
    assert(g_engine.createUDF(db, "metawriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO metadata_calls VALUES(arg); RETURN arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    const std::string column = "\x01routine:.metawriter";
    const std::map<std::string, std::string> hints{{column, "bigint"}};
    const std::string sql = "metawriter(1)+\"" + column + "\"";
    const auto actual = ExprHelper::inferResultType(sql, hints, db, &g_engine);
    std::cout << "routine/column collision actual type=" << actual << std::endl;
    const auto before = g_engine.plpgsqlQuery(db, "SELECT id FROM metadata_calls");
    assert(before.ok && before.rowCount == 0 && !g_engine.inTransaction());
    std::cout << "metadata preparation writes=" << before.rowCount << std::endl;
    assert(actual == "bigint");
    assert(ExprHelper::inferResultType("coalesce(metawriter(1),\"" + column + "\")",
        hints, db, &g_engine) == "bigint");
    assert(ExprHelper::inferResultType("CASE WHEN false THEN metawriter(1) ELSE \"" + column + "\" END",
        hints, db, &g_engine) == "bigint");
    const std::map<std::string, std::string> spoofedRoutineHint{
        {"\x01routine:.abs", "bigint"}, {column, "bigint"}};
    assert(ExprHelper::inferResultType("metawriter(1)+abs(2)",
        spoofedRoutineHint, db, &g_engine) == "integer");
    const auto calls = g_engine.plpgsqlQuery(db, "SELECT id FROM metadata_calls");
    assert(calls.ok && calls.rowCount == 0 && !g_engine.inTransaction());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[STORED FUNCTION METADATA NAMESPACE] passed\n";
}
