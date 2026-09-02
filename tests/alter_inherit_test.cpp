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
    assert(!fs::exists(fs::path(db) / ".child.inherits"));

    {
        dbms::StorageEngine restarted;
        assert(restarted.getInheritedChildren(db, "p1") ==
               std::vector<std::string>{"child"});
    }

    assert(!ddl.executeSql("ALTER TABLE child NO INHERIT p1", s));
    assert(g_engine.getInheritedChildren(db, "p1").empty());
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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_inherit_parser();
    test_inherit_execution();
    test_table_rename_updates_inheritance_graph();
    std::cout << "[INHERIT] all passed" << std::endl;
    return 0;
}
