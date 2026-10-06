#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "stored_function_local_owner";
    const std::string otherName = "stored_function_global_owner";
    const std::string db = testDbPath(name), other = testDbPath(otherName);
    cleanupTestDb(name);
    cleanupTestDb(otherName);
    StorageEngine local;
    assert(local.createDatabase(db, "utf8") == DBStatus::OK);
    assert(g_engine.createDatabase(other, "utf8") == DBStatus::OK);
    TableSchema table;
    table.len = 1;
    table.cols[0].dataName = "id";
    table.cols[0].dataType = "int";
    table.cols[0].dsize = 4;
    assert(local.createTable(db, "driver", table) == DBStatus::OK);
    assert(local.createTable(db, "sink", table) == DBStatus::OK);
    assert(local.insertRow(db, "driver", {{"id", "7"}}) == DBStatus::OK);
    assert(local.createUDF(db, "ownerwriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO sink VALUES(arg); RETURN -arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    assert(g_engine.createUDF(other, "ownerwriter", {"arg"}, {"int"},
        "SELECT 999", 'i', "sql", "int") == DBStatus::OK);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    StorageEngine::OrderBySpec key;
    key.isExpression = true;
    key.exprFunc = "expreval";
    key.expressionSql = "ownerwriter(id)";
    const auto rows = local.query(db, "driver", {}, {"id"}, {key});
    assert(rows.size() == 1 && local.inTransaction() && !g_engine.inTransaction());
    // Another engine must not see a committed child transaction. The
    // function's write belongs to the native caller's existing transaction.
    const auto observer = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(observer.ok && observer.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    const auto empty = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(empty.ok && empty.rowCount == 0);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    const auto legacySorted = local.sortByExpression(db, "driver",
        {"7 ", "8 "}, {key});
    assert(legacySorted.size() == 2 && local.inTransaction() && !g_engine.inTransaction());
    const auto legacyPrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(legacyPrivate.ok && legacyPrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    StorageEngine::SelectExpr projection;
    projection.isScalar = true;
    projection.funcName = "expreval";
    projection.funcArgs = {"ownerwriter(id)"};
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    const auto projected = local.queryExpr(db, "driver", {}, {projection}, {},
        &cells, &nulls);
    assert(projected.size() == 1 && cells.size() == 1 &&
        cells[0][0] == "-7" && !nulls[0][0]);
    const auto projectionPrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(projectionPrivate.ok && projectionPrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    const auto selected = local.query(db, "driver",
        {"typedexpr ownerwriter(id) < 0"}, {"id"});
    assert(selected.size() == 1);
    const auto predicatePrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(predicatePrivate.ok && predicatePrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    TableSchema defaultTable = table;
    defaultTable.cols[0].defaultValue = "ownerwriter(9)";
    assert(local.createTable(db, "defaults", defaultTable) == DBStatus::OK);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    assert(local.insertRow(db, "defaults", {}) == DBStatus::OK);
    const auto defaultPrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(defaultPrivate.ok && defaultPrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    assert(local.createUDF(db, "ownerlookup", {}, {},
        "DECLARE n INT; BEGIN SELECT id INTO n FROM sink; RETURN n; END;",
        'i', "plpgsql", "int") == DBStatus::OK);
    TableSchema generatedTable = table;
    generatedTable.len = 2;
    generatedTable.cols[1] = table.cols[0];
    generatedTable.cols[1].dataName = "computed";
    generatedTable.cols[1].generatedKind = 's';
    generatedTable.cols[1].generatedExpr = "ownerlookup()";
    generatedTable.cols[1].checkExpr = "ownerlookup() IS NOT NULL";
    assert(local.createTable(db, "generated", generatedTable) == DBStatus::OK);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    assert(local.insertRow(db, "sink", {{"id", "13"}}) == DBStatus::OK);
    assert(local.finishSqlCommand());
    assert(local.beginSqlCommand());
    const auto generatedStatus = local.insertRow(db, "generated", {{"id", "1"}});
    if (generatedStatus != DBStatus::OK)
        std::cerr << "generated owner status=" << static_cast<int>(generatedStatus) << '\n';
    assert(generatedStatus == DBStatus::OK);
    const auto generated = local.plpgsqlQuery(db, "SELECT computed FROM generated");
    assert(generated.ok && generated.rowCount == 1 &&
        generated.firstRow[0] && *generated.firstRow[0] == "13");
    const auto generatedPrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(generatedPrivate.ok && generatedPrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    assert(local.createUDF(db, "ownerouter", {}, {},
        "BEGIN RETURN ownerwriter(8); END;", 'v', "plpgsql", "int") == DBStatus::OK);
    assert(local.beginTransaction(db) == DBStatus::OK);
    assert(local.beginSqlCommand());
    std::string value;
    assert(local.callUDF(db, "ownerouter", {}, value) && value == "-8");
    const auto stillPrivate = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(stillPrivate.ok && stillPrivate.rowCount == 0);
    assert(local.rollbackTransaction() == DBStatus::OK);
    std::string otherValue;
    assert(g_engine.callUDF(other, "ownerwriter", {"8"}, otherValue) && otherValue == "999");
    assert(local.dropDatabase(db) == DBStatus::OK);
    assert(g_engine.dropDatabase(other) == DBStatus::OK);
    cleanupTestDb(name);
    cleanupTestDb(otherName);
    std::cout << "[STORED FUNCTION ENGINE OWNER] passed\n";
}
