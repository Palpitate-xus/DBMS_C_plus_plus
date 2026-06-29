// ============================================================================
// CREATE TYPE shell + DROP TYPE enum fix test — Phase 4 Wave 4.30a
// Verifies:
//   1. DROP TYPE now works for enum types (previously only composite was
//      supported, causing "DROP TYPE failed" for enums).
//   2. CREATE TYPE name (no AS clause) creates a shell type and DROP TYPE
//      removes it.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static bool shellTypeFileContains(const std::string& db, const std::string& name) {
    auto path = fs::path(db) / ".shell_types";
    if (!fs::exists(path)) return false;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line == name) return true;
    }
    return false;
}

static void test_drop_enum_type() {
    std::string db = "ct_drop_enum";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", s));
    // Use the enum in a table to ensure it is real.
    assert(!ddl.executeSql("CREATE TABLE t (id INT, m mood)", s));
    // This used to print "DROP TYPE failed" because executeDropType only called
    // dropCompositeType.
    assert(!ddl.executeSql("DROP TYPE mood", s));

    cleanup(db);
    std::cout << "[CTYPE] drop enum type OK" << std::endl;
}

static void test_shell_type_create_drop() {
    std::string db = "ct_shell";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE point_shell", s));
    assert(shellTypeFileContains(db, "point_shell"));

    assert(!ddl.executeSql("DROP TYPE point_shell", s));
    assert(!shellTypeFileContains(db, "point_shell"));

    cleanup(db);
    std::cout << "[CTYPE] shell type create/drop OK" << std::endl;
}

static void test_drop_composite_still_works() {
    std::string db = "ct_drop_comp";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE coord AS (x INT, y INT)", s));
    assert(!ddl.executeSql("DROP TYPE coord", s));

    cleanup(db);
    std::cout << "[CTYPE] drop composite type OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_drop_enum_type();
    test_shell_type_create_drop();
    test_drop_composite_still_works();
    std::cout << "[CTYPE] all passed" << std::endl;
    return 0;
}
