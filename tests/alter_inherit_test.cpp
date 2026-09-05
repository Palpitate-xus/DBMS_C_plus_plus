// ============================================================================
// ALTER TABLE INHERIT test — Phase 4 Wave 4.27
// Tests parser handles INHERIT/NO INHERIT (execution via main.cpp requires
// a Session + full runtime; here we verify DDL round-trips through the
// DdlExecutor + direct storage verification).
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/CatalogService.h"
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

static const dbms::Column* findColumn(const dbms::TableSchema& table,
                                      const std::string& name) {
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == name) return &table.cols[i];
    }
    return nullptr;
}

// Verify parser correctly parses ALTER TABLE ... INHERIT / NO INHERIT.
static void test_inherit_parser() {
    std::string db = testDbPath("inh_parse");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (a INT, b INT)", s));
    assert(!ddl.executeSql("CREATE TABLE child (c INT)", s));

    // Parse ALTER TABLE child INHERIT parent
    dbms::SQLParser parser;
    auto r = parser.parse("ALTER TABLE child INHERIT parent");
    assert(r.success);
    assert(r.stmt);
    auto* alter = dynamic_cast<dbms::AlterTableStmt*>(r.stmt.get());
    assert(alter);
    assert(alter->tableName == "child");
    assert(alter->subCommands.size() == 1);
    assert(alter->subCommands[0].action == dbms::AlterTableStmt::Action::Inherit);
    assert(alter->subCommands[0].parentTable == "parent");

    // Parse ALTER TABLE child NO INHERIT parent
    r = parser.parse("ALTER TABLE child NO INHERIT parent");
    assert(r.success);
    assert(r.stmt);
    alter = dynamic_cast<dbms::AlterTableStmt*>(r.stmt.get());
    assert(alter);
    assert(alter->subCommands.size() == 1);
    assert(alter->subCommands[0].action == dbms::AlterTableStmt::Action::NoInherit);
    assert(alter->subCommands[0].parentTable == "parent");

    cleanup(db);
    std::cout << "[INHERIT] parser OK" << std::endl;
}

// Verify ALTER TABLE INHERIT/NO INHERIT updates the graph consumed by DML.
static void test_inherit_execution() {
    std::string db = testDbPath("inh_exec");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE p1 (a INT)", s));
    assert(!ddl.executeSql("CREATE TABLE child (a INT, c INT)", s));

    assert(!ddl.executeSql("ALTER TABLE child INHERIT p1", s));
    assert(g_engine.getInheritedChildren(db, "p1") ==
           std::vector<std::string>{"child"});
    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* parentRelation =
        catalog.resolveRelation("p1", {"public"});
    assert(parentRelation && parentRelation->relhassubclass);
    const dbms::Oid parentOid = parentRelation->oid;
    assert(!fs::exists(fs::path(db) / ".child.inherits"));

    {
        dbms::StorageEngine restarted;
        assert(restarted.getInheritedChildren(db, "p1") ==
               std::vector<std::string>{"child"});
    }

    assert(!ddl.executeSql("ALTER TABLE child NO INHERIT p1", s));
    assert(g_engine.getInheritedChildren(db, "p1").empty());
    assert(!catalog.findClass(parentOid)->relhassubclass);
    assert(ddl.executeSql("ALTER TABLE child INHERIT missing_parent", s));
    assert(ddl.executeSql("ALTER TABLE missing_child INHERIT p1", s));
    assert(ddl.executeSql("ALTER TABLE child INHERIT child", s));
    assert(!ddl.executeSql("CREATE TABLE incompatible (c INT)", s));
    assert(ddl.executeSql("ALTER TABLE incompatible INHERIT p1", s));

    assert(!ddl.executeSql("CREATE TABLE cycle_a (a INT)", s));
    assert(!ddl.executeSql("CREATE TABLE cycle_b (a INT)", s));
    assert(!ddl.executeSql("ALTER TABLE cycle_b INHERIT cycle_a", s));
    assert(ddl.executeSql("ALTER TABLE cycle_a INHERIT cycle_b", s));

    cleanup(db);
    std::cout << "[INHERIT] execution OK" << std::endl;
}

static void test_table_rename_updates_inheritance_graph() {
    const std::string db = testDbPath("inh_rename");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE child (payload INT) INHERITS (parent)", s));
    auto children = g_engine.getInheritedChildren(db, "parent");
    assert(children == std::vector<std::string>{"child"});

    assert(!ddl.executeSql("ALTER TABLE parent RENAME TO renamed_parent", s));
    assert(g_engine.getInheritedChildren(db, "parent").empty());
    children = g_engine.getInheritedChildren(db, "renamed_parent");
    assert(children == std::vector<std::string>{"child"});

    assert(!ddl.executeSql("ALTER TABLE child RENAME TO renamed_child", s));
    children = g_engine.getInheritedChildren(db, "renamed_parent");
    assert(children == std::vector<std::string>{"renamed_child"});

    {
        dbms::StorageEngine restarted;
        children = restarted.getInheritedChildren(db, "renamed_parent");
        assert(children == std::vector<std::string>{"renamed_child"});
    }

    cleanup(db);
    std::cout << "[INHERIT] table rename preserves graph OK" << std::endl;
}

static void test_inherit_requires_parent_constraints() {
    const std::string db = testDbPath("inh_constraints");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE constrained_parent (id INT NOT NULL, amount INT, "
        "CONSTRAINT amount_positive CHECK (amount > 0))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE nullable_child (id INT, amount INT, "
        "CONSTRAINT amount_positive CHECK (amount > 0))", s));
    assert(ddl.executeSql(
        "ALTER TABLE nullable_child INHERIT constrained_parent", s));

    assert(!ddl.executeSql(
        "CREATE TABLE unchecked_child (id INT NOT NULL, amount INT)", s));
    assert(ddl.executeSql(
        "ALTER TABLE unchecked_child INHERIT constrained_parent", s));

    assert(!ddl.executeSql(
        "CREATE TABLE wrong_check_child (id INT NOT NULL, amount INT, "
        "CONSTRAINT amount_positive CHECK (amount >= 0))", s));
    assert(ddl.executeSql(
        "ALTER TABLE wrong_check_child INHERIT constrained_parent", s));
    assert(g_engine.getInheritedChildren(db, "constrained_parent").empty());

    assert(!ddl.executeSql(
        "CREATE TABLE matching_child (id INT NOT NULL, amount INT, "
        "CONSTRAINT amount_positive CHECK (amount > 0))", s));
    assert(!ddl.executeSql(
        "ALTER TABLE matching_child INHERIT constrained_parent", s));
    assert(g_engine.getInheritedChildren(db, "constrained_parent") ==
           std::vector<std::string>{"matching_child"});

    assert(!ddl.executeSql(
        "CREATE TABLE generated_parent (base INT, derived INT "
        "GENERATED ALWAYS AS (base + 1) STORED)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE plain_generated_child (base INT, derived INT)", s));
    assert(ddl.executeSql(
        "ALTER TABLE plain_generated_child INHERIT generated_parent", s));
    assert(!ddl.executeSql(
        "CREATE TABLE generated_child (base INT, derived INT "
        "GENERATED ALWAYS AS (base + 2) STORED)", s));
    assert(!ddl.executeSql(
        "ALTER TABLE generated_child INHERIT generated_parent", s));

    cleanup(db);
    std::cout << "[INHERIT] parent constraints required OK" << std::endl;
}

static void test_drop_removes_inheritance_edges() {
    const std::string db = testDbPath("inh_drop");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE child (payload INT) INHERITS (parent)", s));
    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* parentRelation =
        catalog.resolveRelation("parent", {"public"});
    assert(parentRelation && parentRelation->relhassubclass);
    const dbms::Oid parentOid = parentRelation->oid;
    assert(!ddl.executeSql("DROP TABLE child", s));
    assert(g_engine.getInheritedChildren(db, "parent").empty());
    assert(!catalog.findClass(parentOid)->relhassubclass);

    // Reusing the child's name without INHERITS must not resurrect the old
    // edge. Dropping the parent must likewise detach a surviving child.
    assert(!ddl.executeSql("CREATE TABLE child (id INT, payload INT)", s));
    assert(g_engine.getInheritedChildren(db, "parent").empty());
    assert(!ddl.executeSql("ALTER TABLE child INHERIT parent", s));
    assert(catalog.findClass(parentOid)->relhassubclass);
    assert(!ddl.executeSql("DROP TABLE parent", s));
    assert(g_engine.getInheritedChildren(db, "parent").empty());
    assert(g_engine.tableExists(db, "child"));
    assert(!ddl.executeSql("CREATE TABLE parent (id INT)", s));
    assert(g_engine.getInheritedChildren(db, "parent").empty());

    cleanup(db);
    std::cout << "[INHERIT] DROP removes graph edges OK" << std::endl;
}

static void test_create_inherits_metadata_failure_is_atomic() {
    const std::string db = testDbPath("inh_create_io_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT)", s));
    const fs::path inheritancePath =
        fs::path(g_engine.dbPath(db)) / ".inherits";
    assert(fs::create_directory(inheritancePath));

    assert(ddl.executeSql(
        "CREATE TABLE child (payload INT) INHERITS (parent)", s));
    assert(!g_engine.tableExists(db, "child"));
    assert(g_engine.getInheritedChildren(db, "parent").empty());
    {
        dbms::StorageEngine restarted;
        assert(!restarted.tableExists(db, "child"));
        assert(restarted.getInheritedChildren(db, "parent").empty());
    }

    cleanup(db);
    std::cout << "[INHERIT] CREATE metadata failure is atomic OK"
              << std::endl;
}

static void test_create_inherits_merges_columns_and_constraints() {
    const std::string db = testDbPath("inh_column_merge");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE parent (id INT NOT NULL UNIQUE, inherited_default INT "
        "DEFAULT 7, parent_identity INT GENERATED ALWAYS AS IDENTITY)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE child (id INT, local_a INT, local_b INT, "
        "PRIMARY KEY (local_a, local_b)) INHERITS (parent)", s));

    const auto child = g_engine.getTableSchema(db, "child");
    assert(child.len == 5);
    const dbms::Column* id = findColumn(child, "id");
    const dbms::Column* inheritedDefault =
        findColumn(child, "inherited_default");
    const dbms::Column* parentIdentity =
        findColumn(child, "parent_identity");
    assert(id && !id->isNull && !id->isUnique);
    assert(inheritedDefault && inheritedDefault->defaultValue == "7");
    assert(parentIdentity && !parentIdentity->isAutoIncrement);
    assert(child.pkColIndices == std::vector<size_t>({3, 4}));
    assert(g_engine.insert(
               db, "child", {{"local_a", "1"}, {"local_b", "2"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);

    assert(ddl.executeSql(
        "CREATE TABLE incompatible (id VARCHAR(8)) INHERITS (parent)", s));
    assert(!g_engine.tableExists(db, "incompatible"));

    assert(!ddl.executeSql("CREATE TABLE defaults_a (shared INT DEFAULT 1)", s));
    assert(!ddl.executeSql("CREATE TABLE defaults_b (shared INT DEFAULT 2)", s));
    assert(ddl.executeSql(
        "CREATE TABLE conflicting (extra INT) "
        "INHERITS (defaults_a, defaults_b)", s));
    assert(!g_engine.tableExists(db, "conflicting"));
    assert(!ddl.executeSql(
        "CREATE TABLE overridden (shared INT DEFAULT 3, extra INT) "
        "INHERITS (defaults_a, defaults_b)", s));
    const auto overridden = g_engine.getTableSchema(db, "overridden");
    const dbms::Column* shared = findColumn(overridden, "shared");
    assert(shared && shared->defaultValue == "3");

    assert(!ddl.executeSql(
        "CREATE TABLE checks_a (value INT, "
        "CONSTRAINT positive CHECK (value > 0), "
        "CONSTRAINT bounded CHECK (value < 10))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE checks_b (value INT, "
        "CONSTRAINT positive CHECK (value > 0), "
        "CONSTRAINT bounded CHECK (value < 10))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE merged_checks (extra INT) "
        "INHERITS (checks_a, checks_b)", s));
    const auto mergedChecks =
        g_engine.getTableSchema(db, "merged_checks");
    size_t mergedCheckCount = mergedChecks.additionalCheckConstraints.size();
    for (size_t columnIndex = 0; columnIndex < mergedChecks.len;
         ++columnIndex) {
        if (!mergedChecks.cols[columnIndex].checkExpr.empty()) {
            ++mergedCheckCount;
        }
    }
    assert(mergedCheckCount == 2);

    assert(!ddl.executeSql(
        "CREATE TABLE checks_conflict (value INT, "
        "CONSTRAINT positive CHECK (value > 0), "
        "CONSTRAINT bounded CHECK (value < 20))", s));
    assert(ddl.executeSql(
        "CREATE TABLE conflicting_checks (extra INT) "
        "INHERITS (checks_a, checks_conflict)", s));
    assert(!g_engine.tableExists(db, "conflicting_checks"));
    assert(ddl.executeSql(
        "CREATE TABLE local_check_conflict "
        "(CONSTRAINT bounded CHECK (value < 20)) INHERITS (checks_a)", s));
    assert(!g_engine.tableExists(db, "local_check_conflict"));

    assert(!ddl.executeSql(
        "CREATE TABLE keyed (a INT, b INT, PRIMARY KEY (a, b))", s));
    assert(!ddl.executeSql("CREATE TABLE prefix (p INT)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE inherited_key "
        "(LIKE keyed INCLUDING INDEXES) INHERITS (prefix)", s));
    const auto inheritedKey =
        g_engine.getTableSchema(db, "inherited_key");
    assert(inheritedKey.pkColIndices == std::vector<size_t>({1, 2}));

    cleanup(db);
    std::cout << "[INHERIT] column and constraint merge semantics OK"
              << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_inherit_parser();
    test_inherit_execution();
    test_inherit_requires_parent_constraints();
    test_table_rename_updates_inheritance_graph();
    test_drop_removes_inheritance_edges();
    test_create_inherits_metadata_failure_is_atomic();
    test_create_inherits_merges_columns_and_constraints();
    std::cout << "[INHERIT] all passed" << std::endl;
    return 0;
}
