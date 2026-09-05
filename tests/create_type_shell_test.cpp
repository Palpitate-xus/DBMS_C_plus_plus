// ============================================================================
// CREATE TYPE shell/range/base + DROP TYPE enum fix test — Phase 4 Wave 4.30
// Verifies:
//   1. DROP TYPE now works for enum types (previously only composite was
//      supported, causing "DROP TYPE failed" for enums).
//   2. CREATE TYPE name (no AS clause) creates a shell type and DROP TYPE
//      removes it.
//   3. CREATE TYPE name AS RANGE (subtype = ...) registers range metadata.
//   4. CREATE TYPE name (INPUT=..., OUTPUT=..., ...) registers base-type
//      metadata.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

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

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static std::map<std::string, std::string> parseUdtMetaLine(const std::string& line) {
    std::map<std::string, std::string> out;
    size_t k1 = line.find('|');
    size_t k2 = line.find('|', k1 + 1);
    if (k1 == std::string::npos || k2 == std::string::npos) return out;
    std::string rest = line.substr(k2 + 1);
    std::stringstream ss(rest);
    std::string kv;
    while (std::getline(ss, kv, ';')) {
        size_t eq = kv.find('=');
        if (eq == std::string::npos) continue;
        out[kv.substr(0, eq)] = kv.substr(eq + 1);
    }
    return out;
}

static bool udtMetaContains(const std::string& db, const std::string& kind, const std::string& name,
                            const std::map<std::string, std::string>& expected) {
    auto path = fs::path(db) / ".udt_meta";
    if (!fs::exists(path)) return false;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.substr(0, kind.size()) != kind) continue;
        size_t p1 = line.find('|');
        size_t p2 = line.find('|', p1 + 1);
        std::string n = (p1 == std::string::npos) ? "" : line.substr(p1 + 1, p2 - p1 - 1);
        if (n != name) continue;
        auto attrs = parseUdtMetaLine(line);
        for (const auto& kv : expected) {
            if (attrs[kv.first] != kv.second) return false;
        }
        return true;
    }
    return false;
}

static void test_drop_enum_type() {
    std::string db = testDbPath("ct_drop_enum");
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
    std::string db = testDbPath("ct_shell");
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

static void test_range_type_create_drop() {
    std::string db = testDbPath("ct_range");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE intrange AS RANGE (subtype = int4, canonical = int4range_canonical)", s));
    assert(udtMetaContains(db, "range", "intrange", {{"subtype", "int4"}, {"canonical", "int4range_canonical"}}));

    // Duplicate create is rejected.
    assert(ddl.executeSql("CREATE TYPE intrange AS RANGE (subtype = int4)", s));

    assert(!ddl.executeSql("DROP TYPE intrange", s));
    assert(!udtMetaContains(db, "range", "intrange", {}));

    cleanup(db);
    std::cout << "[CTYPE] range type create/drop OK" << std::endl;
}

static void test_base_type_create_drop() {
    std::string db = testDbPath("ct_base");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE posint (INPUT = posint_in, OUTPUT = posint_out, CATEGORY = N)", s));
    assert(udtMetaContains(db, "base", "posint", {{"input", "posint_in"}, {"output", "posint_out"}, {"category", "N"}}));

    assert(!ddl.executeSql("DROP TYPE posint", s));
    assert(!udtMetaContains(db, "base", "posint", {}));

    cleanup(db);
    std::cout << "[CTYPE] base type create/drop OK" << std::endl;
}

static void test_drop_composite_still_works() {
    std::string db = testDbPath("ct_drop_comp");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE coord AS (x INT, y INT)", s));
    assert(!ddl.executeSql("DROP TYPE coord", s));

    cleanup(db);
    std::cout << "[CTYPE] drop composite type OK" << std::endl;
}

static void test_schema_qualified_type_names() {
    std::string db = testDbPath("ct_schema_qualified");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql("CREATE SCHEMA neighbor", s));
    assert(ddl.executeSql("CREATE TYPE missing.marker", s));

    assert(!ddl.executeSql("CREATE TYPE app.marker", s));
    assert(!ddl.executeSql("CREATE TYPE neighbor.marker", s));
    assert(shellTypeFileContains(db, "app.marker"));
    assert(shellTypeFileContains(db, "neighbor.marker"));

    assert(!ddl.executeSql(
        "CREATE TYPE app.int_range AS RANGE (subtype = int4)", s));
    assert(udtMetaContains(
        db, "range", "app.int_range", {{"subtype", "int4"}}));
    assert(!ddl.executeSql("CREATE TYPE app.mood AS ENUM ('ok', 'sad')", s));
    assert(g_engine.getEnumType(db, "app.mood").name == "app.mood");
    assert(!ddl.executeSql("CREATE TYPE app.coordinate AS (x int, y int)", s));
    assert(g_engine.isCompositeType(db, "app.coordinate"));

    assert(ddl.executeSql("DROP TYPE app.marker, app.int_range", s));
    assert(shellTypeFileContains(db, "app.marker"));
    assert(udtMetaContains(db, "range", "app.int_range", {}));

    assert(!ddl.executeSql("DROP TYPE app.marker", s));
    assert(!shellTypeFileContains(db, "app.marker"));
    assert(shellTypeFileContains(db, "neighbor.marker"));
    assert(!ddl.executeSql("DROP TYPE app.int_range", s));
    assert(!udtMetaContains(db, "range", "app.int_range", {}));
    assert(!ddl.executeSql("DROP TYPE app.mood", s));
    assert(g_engine.getEnumType(db, "app.mood").name.empty());
    assert(!ddl.executeSql("DROP TYPE app.coordinate", s));
    assert(!g_engine.isCompositeType(db, "app.coordinate"));

    cleanup(db);
    std::cout << "[CTYPE] schema-qualified names OK" << std::endl;
}

static void test_composite_metadata_is_validated_and_atomic() {
    const std::string db = testDbPath("ct_composite_atomicity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::StorageEngine::CompositeType first;
    first.name = "first_pair";
    first.fields = {{"left_value", "int"}, {"right_value", "text"}};
    dbms::StorageEngine::CompositeType second = first;
    second.name = "second_pair";
    assert(g_engine.createCompositeType(db, first) == dbms::DBStatus::OK);
    assert(g_engine.createCompositeType(db, second) == dbms::DBStatus::OK);

    dbms::StorageEngine::CompositeType invalid = first;
    invalid.name = "invalid_pair";
    invalid.fields[0].first = "left|injected";
    assert(g_engine.createCompositeType(db, invalid) ==
           dbms::DBStatus::INVALID_ARGUMENT);

    const fs::path path = fs::path(db) / ".types";
    const fs::path savedPath = fs::path(db) / ".types.saved";
    const std::string originalBytes = readFile(path);
    fs::rename(path, savedPath);
    assert(fs::create_directory(path));
    dbms::StorageEngine::CompositeType changed = first;
    changed.fields[1].second = "varchar(40)";
    assert(g_engine.alterCompositeType(db, first.name, changed) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.dropCompositeType(db, first.name) ==
           dbms::DBStatus::IO_ERROR);
    fs::remove(path);
    fs::rename(savedPath, path);
    assert(readFile(path) == originalBytes);

    assert(g_engine.alterCompositeType(db, first.name, changed) ==
           dbms::DBStatus::OK);
    dbms::StorageEngine reloaded;
    const auto persisted = reloaded.getCompositeType(db, first.name);
    assert(persisted.fields.size() == 2);
    assert(persisted.fields[1].second == "varchar(40)");
    assert(reloaded.isCompositeType(db, second.name));

    assert(g_engine.dropCompositeType(db, first.name) ==
           dbms::DBStatus::OK);
    dbms::StorageEngine afterDrop;
    assert(!afterDrop.isCompositeType(db, first.name));
    assert(afterDrop.isCompositeType(db, second.name));

    cleanup(db);
    std::cout << "[CTYPE] composite metadata atomicity OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_drop_enum_type();
    test_shell_type_create_drop();
    test_range_type_create_drop();
    test_base_type_create_drop();
    test_drop_composite_still_works();
    test_schema_qualified_type_names();
    test_composite_metadata_is_validated_and_atomic();
    std::cout << "[CTYPE] all passed" << std::endl;
    return 0;
}
