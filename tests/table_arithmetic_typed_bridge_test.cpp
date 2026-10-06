#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

static dbms::StorageEngine::SelectExpr arithmetic(std::vector<std::string> arguments) {
    dbms::StorageEngine::SelectExpr result;
    result.isScalar = true;
    result.funcName = "arith";
    result.funcArgs = std::move(arguments);
    return result;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "table_arithmetic_typed_bridge", db = testDbPath(name);
    cleanupTestDb(name);
    StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.tablename = "values_";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 2));
    table.append(makeFloatColumn("f", false));
    table.append(makeFloatColumn("g", false));
    table.append(makeDoubleColumn("d", false));
    table.append(makeDoubleColumn("e", false));
    table.append(makeIntColumn("i", false, 2));
    table.append(makeIntColumn("s", false, 0));
    table.append(makeIntColumn("b", false, 8));
    table.append(makeVarCharColumn("t", false, 16));
    for (size_t column = 1; column < table.len; ++column) table.cols[column].isNull = true;
    assert(engine.createTable(db, table) == DBStatus::OK);
    assert(engine.insertRow(db, "values_", {{"id","1"},{"f","16777216"},{"g","1"},{"d","9007199254740992"},{"e","1"},{"i","2147483647"},{"s","32767"},{"b","9223372036854775807"},{"t",""}}) == DBStatus::OK);
    assert(engine.insertRow(db, "values_", {{"id","2"},{"f",std::nullopt},{"g",std::nullopt},{"d",std::nullopt},{"e",std::nullopt},{"i",std::nullopt},{"s",std::nullopt},{"b",std::nullopt},{"t",std::nullopt}}) == DBStatus::OK);
    std::vector<std::vector<std::string>> rows;
    std::vector<std::vector<bool>> nulls;
    engine.queryExpr(db, "values_", {}, {arithmetic({"f", "+", "g"}), arithmetic({"d", "+", "e"}), arithmetic({"b", "+", "0"}), arithmetic({"t", "||", "''"})}, {}, &rows, &nulls);
    assert(rows.size() == 2 && nulls.size() == 2);
    if (std::stod(rows[0][0]) != 16777216) {
        std::cerr << "REAL+REAL=" << rows[0][0] << '\n';
        assert(false);
    }
    assert(std::stod(rows[0][1]) == 9007199254740992.0 && rows[0][2] == "9223372036854775807");
    assert(rows[0][3].empty() && !nulls[0][3]);
    for (const bool isNull : nulls[1]) assert(isNull);
    const auto error = [&](std::vector<std::string> arguments, const std::string& state) {
        bool rejected = false;
        try {
            engine.queryExpr(db, "values_", {"=id 1"}, {arithmetic(std::move(arguments))});
        } catch (const DbError& failure) {
            rejected = failure.sqlState() == state;
        } catch (const std::exception& failure) {
            rejected = std::string(failure.what()).find("SQLSTATE " + state) != std::string::npos;
        }
        assert(rejected);
        assert(engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    error({"i", "+", "1"}, "22003");
    error({"s", "+", "s"}, "22003");
    error({"i", "/", "0"}, "22012");
    TableSchema sink;
    sink.tablename = "sink";
    sink.append(makeIntColumn("id", false, 2));
    assert(engine.createTable(db, sink) == DBStatus::OK);
    assert(engine.createUDF(db, "arith_writer", {"v"}, {"int"}, "BEGIN INSERT INTO sink VALUES(v); RETURN v; END;", 'v', "plpgsql", "int") == DBStatus::OK);
    assert(ExprHelper::inferResultType("arith_writer(id)+1", {{"id", "int"}}, db, &engine) == "integer");
    assert(g_engine.plpgsqlQuery(db, "SELECT id FROM sink").rowCount == 0);
    assert(engine.beginTransaction(db) == DBStatus::OK && engine.beginSqlCommand());
    engine.queryExpr(db, "values_", {"=id 1"}, {arithmetic({"arith_writer(id)", "+", "1"})}, {}, &rows, &nulls);
    assert(rows.size() == 1 && rows[0][0] == "2" && !nulls[0][0]);
    const auto observer = g_engine.plpgsqlQuery(db, "SELECT id FROM sink");
    assert(observer.ok && observer.rowCount == 0 && !g_engine.inTransaction());
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.plpgsqlQuery(db, "SELECT id FROM sink").rowCount == 0);
    assert(engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[TABLE ARITHMETIC TYPED BRIDGE] width, errors, NULL and owner passed\n";
}
