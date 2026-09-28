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
#include "commands/DdlTransaction.h"
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

static bool decodeHex(const std::string& encoded, std::string& value) {
    if (encoded.size() % 2 != 0) return false;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    value.clear();
    for (size_t i = 0; i < encoded.size(); i += 2) {
        const int high = nibble(encoded[i]);
        const int low = nibble(encoded[i + 1]);
        if (high < 0 || low < 0) return false;
        value.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

static bool parseUdtMetaLine(const std::string& line, std::string& kind,
                             std::string& name,
                             std::map<std::string, std::string>& attrs) {
    kind.clear();
    name.clear();
    attrs.clear();
    static const std::string prefix = "DBMS_UDT_V2:";
    if (line.rfind(prefix, 0) == 0) {
        std::vector<std::string> fields;
        size_t position = prefix.size();
        while (true) {
            const size_t separator = line.find('|', position);
            fields.push_back(separator == std::string::npos
                ? line.substr(position)
                : line.substr(position, separator - position));
            if (separator == std::string::npos) break;
            position = separator + 1;
        }
        if (fields.size() < 2 || (fields.size() - 2) % 2 != 0 ||
            !decodeHex(fields[0], kind) || !decodeHex(fields[1], name)) {
            return false;
        }
        for (size_t i = 2; i < fields.size(); i += 2) {
            std::string key;
            std::string value;
            if (!decodeHex(fields[i], key) ||
                !decodeHex(fields[i + 1], value)) return false;
            attrs[key] = value;
        }
        return true;
    }

    const size_t kindEnd = line.find('|');
    const size_t nameEnd = kindEnd == std::string::npos
        ? std::string::npos : line.find('|', kindEnd + 1);
    if (kindEnd == std::string::npos || nameEnd == std::string::npos) {
        return false;
    }
    kind = line.substr(0, kindEnd);
    name = line.substr(kindEnd + 1, nameEnd - kindEnd - 1);
    std::stringstream attributes(line.substr(nameEnd + 1));
    std::string attribute;
    while (std::getline(attributes, attribute, ';')) {
        const size_t equals = attribute.find('=');
        if (equals == std::string::npos) continue;
        attrs[attribute.substr(0, equals)] = attribute.substr(equals + 1);
    }
    return true;
}

static bool udtMetaContains(const std::string& db, const std::string& kind, const std::string& name,
                            const std::map<std::string, std::string>& expected) {
    auto path = fs::path(db) / ".udt_meta";
    if (!fs::exists(path)) return false;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        std::string storedKind;
        std::string storedName;
        std::map<std::string, std::string> attrs;
        if (!parseUdtMetaLine(line, storedKind, storedName, attrs) ||
            storedKind != kind || storedName != name) continue;
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
    // RESTRICT must protect the table column that depends on this enum.
    assert(ddl.executeSql("DROP TYPE mood", s));
    assert(!ddl.executeSql("DROP TABLE t", s));
    // With no dependents, enum DROP TYPE must work as well as composite DROP.
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

enum class RollbackTypeKind { Shell, Range, Base, Enum, Composite };

static bool rollbackTypeExists(const std::string& db,
                               RollbackTypeKind kind,
                               const std::string& name) {
    switch (kind) {
        case RollbackTypeKind::Shell:
            return shellTypeFileContains(db, name);
        case RollbackTypeKind::Range:
            return udtMetaContains(db, "range", name, {});
        case RollbackTypeKind::Base:
            return udtMetaContains(db, "base", name, {});
        case RollbackTypeKind::Enum:
            return !g_engine.getEnumType(db, name).name.empty();
        case RollbackTypeKind::Composite:
            return g_engine.isCompositeType(db, name);
    }
    return false;
}

static void assert_failed_commit_rolls_back_type(
    const std::string& suffix, const std::string& createSql,
    RollbackTypeKind kind, const std::string& name) {
    const std::string db = testDbPath("ct_rollback_" + suffix);
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE TABLE guard (id int primary key, value int, "
        "CONSTRAINT positive CHECK (value > 0) "
        "DEFERRABLE INITIALLY DEFERRED)", s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "guard", {{"id", "1"}, {"value", "0"}}) ==
           dbms::DBStatus::OK);
    assert(!ddl.executeSql(createSql, s));
    assert(rollbackTypeExists(db, kind, name));
    assert(g_engine.commitTransaction() != dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());
    assert(!rollbackTypeExists(db, kind, name));
    assert(!fs::exists(db + ".txn_backup"));

    cleanup(db);
}

static void test_create_type_commit_failure_restores_all_families() {
    assert_failed_commit_rolls_back_type(
        "shell", "CREATE TYPE app.failed_shell",
        RollbackTypeKind::Shell, "app.failed_shell");
    assert_failed_commit_rolls_back_type(
        "range", "CREATE TYPE app.failed_range AS RANGE (subtype = int4)",
        RollbackTypeKind::Range, "app.failed_range");
    assert_failed_commit_rolls_back_type(
        "base", "CREATE TYPE app.failed_base "
                "(INPUT = failed_in, OUTPUT = failed_out)",
        RollbackTypeKind::Base, "app.failed_base");
    assert_failed_commit_rolls_back_type(
        "enum", "CREATE TYPE app.failed_enum AS ENUM ('one', 'two')",
        RollbackTypeKind::Enum, "app.failed_enum");
    assert_failed_commit_rolls_back_type(
        "composite", "CREATE TYPE app.failed_pair AS (x int, y text)",
        RollbackTypeKind::Composite, "app.failed_pair");
    std::cout << "[CTYPE] failed commit restores every type family OK"
              << std::endl;
}

static bool executeWithDatabaseWritesBlocked(
    dbms::DdlExecutor& ddl, Session& session, const std::string& db,
    const std::string& sql) {
    dbms::DdlTransaction outer(session);
    assert(outer.enableSnapshotRollback());
    assert(outer.begin());
    const fs::perms originalPermissions = fs::status(db).permissions();
    fs::permissions(db, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);
    const bool failed = ddl.executeSql(sql, session);
    fs::permissions(db, originalPermissions, fs::perm_options::replace);
    outer.rollback();
    assert(!g_engine.inTransaction());
    assert(!g_engine.hasTransactionBackup());
    return failed;
}

static void test_type_sidecars_are_atomic_and_fail_closed() {
    const std::string db = testDbPath("ct_sidecar_atomicity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    const fs::path shellPath = fs::path(db) / ".shell_types";
    const fs::path udtPath = fs::path(db) / ".udt_meta";
    {
        std::ofstream shell(shellPath, std::ios::binary);
        shell << "legacy_shell\nneighbor_shell\n";
        assert(shell.good());
        std::ofstream udt(udtPath, std::ios::binary);
        udt << "range|legacy_range|subtype=int4;canonical=a=b|c\n";
        assert(udt.good());
    }

    assert(!ddl.executeSql("CREATE TYPE added_shell", s));
    assert(!ddl.executeSql(
        "CREATE TYPE added_range AS RANGE (subtype = int8)", s));
    assert(shellTypeFileContains(db, "legacy_shell"));
    assert(shellTypeFileContains(db, "added_shell"));
    assert(udtMetaContains(
        db, "range", "legacy_range",
        {{"subtype", "int4"}, {"canonical", "a=b|c"}}));
    assert(udtMetaContains(
        db, "range", "added_range", {{"subtype", "int8"}}));
    const std::string shellBytes = readFile(shellPath);
    const std::string udtBytes = readFile(udtPath);
    assert(udtBytes.rfind("DBMS_UDT_V2:", 0) == 0);
    assert(udtBytes.find("range|legacy_range") == std::string::npos);
    assert(!ddl.executeSql("CREATE TYPE IF NOT EXISTS legacy_shell", s));
    assert(ddl.executeSql(
        "CREATE TYPE legacy_shell AS ENUM ('duplicate_kind')", s));
    assert(!ddl.executeSql("DROP TYPE IF EXISTS absent_type", s));
    assert(readFile(shellPath) == shellBytes);
    assert(readFile(udtPath) == udtBytes);

    assert(executeWithDatabaseWritesBlocked(
        ddl, s, db, "CREATE TYPE blocked_shell"));
    assert(readFile(shellPath) == shellBytes);
    assert(readFile(udtPath) == udtBytes);
    assert(!shellTypeFileContains(db, "blocked_shell"));

    assert(executeWithDatabaseWritesBlocked(
        ddl, s, db, "DROP TYPE legacy_range"));
    assert(readFile(shellPath) == shellBytes);
    assert(readFile(udtPath) == udtBytes);
    assert(udtMetaContains(db, "range", "legacy_range", {}));

    const fs::path compositePath = fs::path(db) / ".types";
    assert(fs::create_directory(compositePath));
    assert(ddl.executeSql("DROP TYPE legacy_shell", s));
    assert(shellTypeFileContains(db, "legacy_shell"));
    assert(fs::remove(compositePath));

    const fs::path savedUdtPath = fs::path(db) / ".udt_meta.saved";
    fs::rename(udtPath, savedUdtPath);
    {
        std::ofstream corrupt(udtPath, std::ios::binary);
        corrupt << "malformed record\n";
        assert(corrupt.good());
    }
    assert(ddl.executeSql("CREATE TYPE hidden_by_corruption", s));
    assert(ddl.executeSql("DROP TYPE legacy_shell", s));
    assert(shellTypeFileContains(db, "legacy_shell"));
    fs::remove(udtPath);
    fs::rename(savedUdtPath, udtPath);

    assert(!ddl.executeSql("DROP TYPE legacy_shell", s));
    assert(!shellTypeFileContains(db, "legacy_shell"));
    assert(shellTypeFileContains(db, "neighbor_shell"));
    assert(!ddl.executeSql("DROP TYPE legacy_range", s));
    assert(!udtMetaContains(db, "range", "legacy_range", {}));

    cleanup(db);
    std::cout << "[CTYPE] sidecar atomicity and fail-closed errors OK"
              << std::endl;
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
    test_create_type_commit_failure_restores_all_families();
    test_type_sidecars_are_atomic_and_fail_closed();
    test_composite_metadata_is_validated_and_atomic();
    std::cout << "[CTYPE] all passed" << std::endl;
    return 0;
}
