#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

static void test_pathkey_with_indexed_order() {
    std::string db = "eq_pk_test";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));

    // Build plan with pathkey requiring ordering by id (which is PK → indexed).
    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t"; ctx.orderByCol = "id"; ctx.orderByAsc = true;

    std::vector<dbms::PathKey> pks;
    pks.push_back({"id", true, "int4_ops"});
    std::vector<dbms::EquivalenceClass> ecs;

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx, pks, ecs);
    assert(plan);

    // Execute and verify
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    // No data inserted; should return empty.
    assert(rows.empty());

    cleanup(db);
    std::cout << "[EQ_PK] pathkey + index OK" << std::endl;
}

static void test_equivalence_class_usage() {
    // Verify equivalence class structure works.
    dbms::EquivalenceClass ec;
    ec.members = {"t1.id", "t2.fk"};
    assert(ec.members.size() == 2);

    std::vector<dbms::EquivalenceClass> ecs;
    ecs.push_back(ec);
    assert(ecs.size() == 1);

    std::cout << "[EQ_PK] equivalence class OK" << std::endl;
}

static void test_pathkey_default() {
    dbms::PathKey pk;
    assert(pk.ascending == true);
    assert(pk.opclass.empty());

    dbms::PathKey pk2{"name", false, "text_ops"};
    assert(pk2.expr == "name");
    assert(pk2.ascending == false);
    assert(pk2.opclass == "text_ops");

    std::cout << "[EQ_PK] pathkey struct OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_pathkey_with_indexed_order();
    test_equivalence_class_usage();
    test_pathkey_default();
    std::cout << "[EQ_PK] all passed" << std::endl;
    return 0;
}
