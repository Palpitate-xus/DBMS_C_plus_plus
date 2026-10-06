#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    for (const std::string expression : {
            "sum('abc')", "avg(NULL)", "pg_catalog.sum('abc')",
            "coalesce(1,sum('abc'))"}) {
        const auto result = ExprHelper::evalString(expression, {});
        if (result.ok || result.error.find("SQLSTATE 42725") == std::string::npos)
            std::cerr << expression << ": " << result.error << '\n';
        assert(!result.ok && result.error.find("SQLSTATE 42725") != std::string::npos);
    }
    for (const std::string expression : {
            "sum('abc'::text)", "wrong_schema.sum('abc')",
            "pg_catalog.\"SUM\"('abc')"}) {
        const auto result = ExprHelper::evalString(expression, {});
        assert(!result.ok && result.error.find("SQLSTATE 42883") != std::string::npos);
    }
    const std::string name = "scalar_aggregate_ambiguity";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    for (const std::string language : {"sql", "plpgsql"}) {
        const std::string fn = "ambiguous_" + language;
        assert(g_engine.createUDF(db, fn, {}, {}, language == "sql"
            ? "SELECT pg_catalog.sum('abc')"
            : "BEGIN RETURN pg_catalog.sum('abc'); END;",
            'v', language, "int") == DBStatus::OK);
        std::string value;
        try {
            g_engine.callUDF(db, fn, {}, value);
            assert(false && "ambiguous aggregate must fail during preparation");
        } catch (const DbError& error) {
            assert(error.sqlState() == "42725");
        }
        assert(!g_engine.inTransaction());
    }
    TableSchema table;
    table.len = 1;
    table.cols[0].dataName = "id";
    table.cols[0].dataType = "int";
    table.cols[0].dsize = 4;
    assert(g_engine.createTable(db, "prep_writes", table) == DBStatus::OK);
    assert(g_engine.createUDF(db, "prepwriter", {"arg"}, {"int"},
        "BEGIN INSERT INTO prep_writes VALUES(arg); RETURN arg; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    auto result = ExprHelper::evalString("coalesce(prepwriter(1),sum('abc'))", {}, {}, db);
    assert(!result.ok && result.error.find("SQLSTATE 42725") != std::string::npos);
    const auto writes = g_engine.plpgsqlQuery(db, "SELECT id FROM prep_writes");
    assert(writes.ok && writes.rowCount == 0 && !g_engine.inTransaction());
    assert(g_engine.createUDF(db, "SUM", {"arg"}, {"text"}, "SELECT 42",
        'i', "sql", "int") == DBStatus::OK);
    assert(g_engine.createUDF(db, "sum", {"arg"}, {"text"}, "SELECT 73",
        'i', "sql", "int") == DBStatus::OK);
    result = ExprHelper::evalString("\"SUM\"('abc')", {}, {}, db);
    assert(result.ok && result.value == "42");
    result = ExprHelper::evalString("public.sum('abc')", {}, {}, db);
    assert(result.ok && result.value == "73");
    result = ExprHelper::evalString("pg_catalog.sum('abc')", {}, {}, db);
    assert(!result.ok && result.error.find("SQLSTATE 42725") != std::string::npos);
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SCALAR AGGREGATE AMBIGUITY] passed\n";
}
