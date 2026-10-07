#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    auto& types = dbms::TypeRegistry::instance();
    types.bootstrap();
    std::cerr << "bpchar canonical " << types.normalizeTypeName("bpchar") << '\n';
    assert(types.normalizeTypeName("bpchar") == "character");
    assert(types.findType("bpchar") == types.findType("character"));
    assert(dbms::ExprHelper::canonicalResultTypeName("bpchar[]") ==
           dbms::ExprHelper::canonicalResultTypeName("CHAR(3)[]"));
    // SQL CHAR is not PostgreSQL's separately quoted internal byte type.
    assert(types.normalizeTypeName("\"char\"").empty());
    const std::string name = "bpchar_array_type_alias";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(c CHAR(3)[],s TEXT[])", session));
    assert(g_engine.insertRow(database, "t", {{"c",std::nullopt},
        {"s",std::string("{\"NULL\",\"\"}")}}) == dbms::DBStatus::OK);
    auto query = g_engine.prepareBoundQuery(database, "SELECT * FROM t");
    const auto schema = g_engine.getTableSchema(database, "t");
    assert(query.sourceRanges.size() == 1);
    assert(query.sourceRanges[0].columns[0].type ==
           dbms::ExprHelper::canonicalResultTypeName(schema.cols[0].dataType+"[]"));
    assert(query.output[0].type == "character[]");
    auto plan = dbms::QueryPlanner::buildPreparedSelectPlan(&g_engine, database, "t", std::move(query));
    const auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
    result.throwIfFailed();
    assert(result.ok && result.structuredRows.size() == 1);
    // A SQL NULL's payload is opaque; the bitmap carries its identity. An
    // actual array containing literal TEXT 'NULL' and '' is not SQL NULL.
    assert(result.structuredNulls == std::vector<std::vector<bool>>({{true,false}}));
    assert(result.structuredRows[0][1] == "{\"NULL\",\"\"}");
    assert(!ddl.executeSql("DROP TABLE t", session));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[BPCHAR ARRAY TYPE ALIAS] passed\n";
}
