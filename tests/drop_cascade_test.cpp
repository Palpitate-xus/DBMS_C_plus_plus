#include "commands/DdlExecutor.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_drop_table_cascade_removes_dependents() {
    std::string db = testDbPath("drop_cascade_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", s);
    ddl.executeSql("CREATE TABLE child (id INT, pid INT)", s);

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);

    const auto* parentCls = cat.findClassByName("parent", nsPublic->oid);
    const auto* childCls = cat.findClassByName("child", nsPublic->oid);
    assert(parentCls != nullptr);
    assert(childCls != nullptr);

    // Record an artificial dependency: child -> parent.
    dbms::PgDependRow dep;
    dep.classid = dbms::PgClassOid_Class;
    dep.objid = childCls->oid;
    dep.objsubid = 0;
    dep.refclassid = dbms::PgClassOid_Class;
    dep.refobjid = parentCls->oid;
    dep.refobjsubid = 0;
    dep.deptype = 'n';
    cat.addDepend(dep);

    // RESTRICT should fail because child depends on parent.
    bool err = ddl.executeSql("DROP TABLE parent", s);
    assert(err);
    assert(g_engine.tableExists(db, "parent"));
    assert(cat.findClassByName("parent", nsPublic->oid) != nullptr);

    // CASCADE should drop parent from both catalog and storage, and remove
    // the dependent child row from the catalog. The child's storage file is
    // not touched by this catalog-only dependency pass.
    err = ddl.executeSql("DROP TABLE parent CASCADE", s);
    assert(!err);
    assert(!g_engine.tableExists(db, "parent"));
    assert(cat.findClassByName("parent", nsPublic->oid) == nullptr);
    assert(cat.findClassByName("child", nsPublic->oid) == nullptr);

    // Evict the catalog so the global engine destructor does not recreate
    // the database directory after cleanup().
    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] CASCADE removes dependents OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_drop_table_cascade_removes_dependents();
    std::cout << "[DROP-CASCADE] all passed" << std::endl;
    return 0;
}
