#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "catalog/type_registry.h"
#include <algorithm>
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

static void test_create_view() {
    std::string db = testDbPath("view_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE VIEW v AS SELECT id, name FROM t", s));
    assert(g_engine.viewExists(db, "v"));
    std::string sql = g_engine.getViewSQL(db, "v");
    assert(!sql.empty());
    assert(g_engine.getViewBaseTable(db, "v") == "t");

    cleanup(db);
    std::cout << "[VIEW] create/select OK" << std::endl;
}

static void test_create_or_replace_view() {
    std::string db = testDbPath("view_replace");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE VIEW v AS SELECT id, name FROM t", s));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* initialView = catalog.resolveRelation("v", {"public"});
    assert(initialView != nullptr && initialView->relkind == 'v');
    const dbms::Oid viewOid = initialView->oid;
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW v AS SELECT id, score FROM t", s));

    assert(g_engine.getViewBaseTable(db, "v") == "t");
    std::string sql = g_engine.getViewSQL(db, "v");
    assert(sql.find("score") != std::string::npos);
    const auto* replacedView = catalog.resolveRelation("v", {"public"});
    assert(replacedView != nullptr && replacedView->oid == viewOid);
    const auto attributes = catalog.findAttributes(viewOid);
    assert(attributes.size() == 2);
    assert(attributes[0].attname == "id");
    assert(attributes[1].attname == "score");

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[VIEW] or replace OK" << std::endl;
}

static void test_view_with_check_option() {
    std::string db = testDbPath("view_wco");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(!ddl.executeSql("CREATE VIEW v AS SELECT * FROM t WHERE score > 0 WITH CHECK OPTION", s));
    assert(g_engine.viewExists(db, "v"));

    cleanup(db);
    std::cout << "[VIEW] with check option OK" << std::endl;
}

static void test_view_existence_requires_metadata_file() {
    const std::string db = testDbPath("view_metadata_kind");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const fs::path fakeView = g_engine.viewsDir(db) / "fake.view";
    fs::create_directories(fakeView);
    assert(!g_engine.viewExists(db, "fake"));
    assert(g_engine.getViewSQL(db, "fake").empty());

    cleanup(db);
    std::cout << "[VIEW] metadata object type validation OK" << std::endl;
}

static void test_schema_qualified_view_name() {
    const std::string db = testDbPath("view_qualified_name");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));
    assert(ddl.executeSql(
        "CREATE VIEW missing_schema.v AS SELECT id FROM t", s));
    assert(!g_engine.viewExists(db, "missing_schema.v"));
    assert(!g_engine.viewExists(db, "v"));

    assert(!ddl.executeSql("CREATE SCHEMA reporting", s));
    assert(!ddl.executeSql(
        "CREATE VIEW reporting.v AS SELECT id FROM t", s));
    assert(g_engine.viewExists(db, "reporting.v"));
    assert(!g_engine.viewExists(db, "v"));
    assert(g_engine.getViewBaseTable(db, "reporting.v") == "t");
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* reporting = catalog.findNamespaceByName("reporting");
    assert(reporting != nullptr);
    const auto* view = catalog.findClassByName("v", reporting->oid);
    assert(view != nullptr && view->relkind == 'v');
    assert(!ddl.executeSql("DROP VIEW reporting.v", s));
    assert(catalog.findClassByName("v", reporting->oid) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[VIEW] schema-qualified name OK" << std::endl;
}

static void test_view_catalog_drop_and_table_cascade() {
    const std::string db = testDbPath("view_catalog_lifecycle");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE base (id INT, label VARCHAR(30))", s));
    assert(!ddl.executeSql(
        "CREATE VIEW visible_base AS SELECT id, label FROM base", s));

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* base = catalog.resolveRelation("base", {"public"});
    const auto* view = catalog.resolveRelation("visible_base", {"public"});
    assert(base != nullptr);
    assert(view != nullptr && view->relkind == 'v' && view->relnatts == 2);
    const dbms::Oid baseOid = base->oid;
    const dbms::Oid viewOid = view->oid;
    const auto attributes = catalog.findAttributes(viewOid);
    assert(attributes.size() == 2);
    assert(attributes[0].attname == "id");
    assert(attributes[1].attname == "label");
    const auto dependencies =
        catalog.findDepends(dbms::PgClassOid_Class, viewOid);
    assert(std::any_of(
        dependencies.begin(), dependencies.end(),
        [&](const dbms::PgDependRow& dependency) {
            return dependency.refclassid == dbms::PgClassOid_Class &&
                   dependency.refobjid == baseOid;
        }));

    assert(ddl.executeSql("DROP TABLE base", s));
    assert(g_engine.tableExists(db, "base"));
    assert(g_engine.viewExists(db, "visible_base"));
    assert(!ddl.executeSql("DROP TABLE base CASCADE", s));
    assert(!g_engine.tableExists(db, "base"));
    assert(!g_engine.viewExists(db, "visible_base"));
    assert(catalog.findClass(baseOid) == nullptr);
    assert(catalog.findClass(viewOid) == nullptr);
    assert(catalog.findAttributes(viewOid).empty());

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    assert(reloaded.findClass(baseOid) == nullptr);
    assert(reloaded.findClass(viewOid) == nullptr);

    assert(!ddl.executeSql(
        "CREATE TABLE drop_base (id INT)", s));
    assert(!ddl.executeSql(
        "CREATE VIEW drop_view AS SELECT id FROM drop_base", s));
    const auto* dropView = reloaded.resolveRelation("drop_view", {"public"});
    assert(dropView != nullptr && dropView->relkind == 'v');
    const dbms::Oid dropViewOid = dropView->oid;
    bool handled = false;
    bool error = dbms::tryDdlBridge(
        "drop view drop_view", dbms::SqlCommand::DropView,
        s, handled);
    assert(handled && !error);
    assert(!g_engine.viewExists(db, "drop_view"));
    assert(reloaded.findClass(dropViewOid) == nullptr);
    assert(reloaded.findAttributes(dropViewOid).empty());
    error = dbms::tryDdlBridge(
        "drop view if exists drop_view", dbms::SqlCommand::DropView,
        s, handled);
    assert(handled && !error);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[VIEW] catalog lifecycle and CASCADE OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_view();
    test_create_or_replace_view();
    test_view_with_check_option();
    test_view_existence_requires_metadata_file();
    test_schema_qualified_view_name();
    test_view_catalog_drop_and_table_cascade();
    std::cout << "[VIEW] all passed" << std::endl;
    return 0;
}
