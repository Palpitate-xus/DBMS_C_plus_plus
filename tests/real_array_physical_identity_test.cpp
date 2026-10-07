#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "real_array_physical_identity";
    cleanupTestDb(name); const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database,"utf8") == DBStatus::OK);
    Session session; session.username="testuser";session.permission=1;session.currentDB=database;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(r REAL[],d DOUBLE PRECISION[],x REAL)",session));
    auto query = g_engine.prepareBoundQuery(database,"SELECT * FROM t");
    const auto schema = g_engine.getTableSchema(database,"t");
    std::cerr << "REAL_CATALOG_DESCRIPTOR " << query.output[0].type << " PHYSICAL " << schema.cols[0].dataType << '\n';
    assert(query.output[0].type == "real[]");
    assert(query.output[1].type == "double precision[]");
    assert(query.output[2].type == "real");
    auto& catalog = g_engine.catalogService().get(database);
    const auto* relation = catalog.findClassByName("t",2200);
    assert(relation);
    const auto* realArray = catalog.findAttribute(relation->oid,"r");
    const auto* doubleArray = catalog.findAttribute(relation->oid,"d");
    const auto* scalar = catalog.findAttribute(relation->oid,"x");
    assert(realArray && doubleArray && scalar);
    std::cerr << "REAL_ARRAY_SCALAR_OIDS " << realArray->atttypid << ' ' << doubleArray->atttypid << ' ' << scalar->atttypid << '\n';
    assert(realArray->attnum==1 && doubleArray->attnum==2 && scalar->attnum==3);
    assert(realArray->atttypid==700 && doubleArray->atttypid==701 && scalar->atttypid==700);
    assert(g_engine.insertRow(database,"t",{{"r",std::nullopt},{"d",std::nullopt},{"x",std::nullopt}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"t",{{"r","{1.25,2.5}"},{"d","{1.25,2.5}"},{"x","1.5"}})==DBStatus::OK);
    auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
        &g_engine,database,"t",std::move(query)));
    result.throwIfFailed();
    assert(result.ok && result.structuredRows.size()==2);
    assert(result.structuredNulls == std::vector<std::vector<bool>>({{true,true,true},{false,false,false}}));
    assert(result.structuredRows[1]==std::vector<std::string>({"{1.25,2.5}","{1.25,2.5}","1.5"}));
    assert(!ddl.executeSql("DROP TABLE t",session));
    setCurrentSession(nullptr);cleanupTestDb(name);finalCleanupTestData();
    std::cout << "[REAL ARRAY PHYSICAL IDENTITY] passed\n";
}
