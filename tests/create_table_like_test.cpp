#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "table_schema.h"
#include <cassert>
#include <filesystem>
#include <fstream>
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

static const dbms::Column* findCol(const dbms::TableSchema& t, const std::string& name) {
    for (size_t i = 0; i < t.len; ++i)
        if (t.cols[i].dataName == name) return &t.cols[i];
    return nullptr;
}

static void test_like_basic() {
    std::string db = testDbPath("like_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL DEFAULT 'x')", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    assert(schema.len == 2);
    const dbms::Column* id = findCol(schema, "id");
    const dbms::Column* name = findCol(schema, "name");
    assert(id && name);
    // Plain LIKE: column structure + NOT NULL copied, but NOT PK or DEFAULT.
    assert(id->isPrimaryKey == false);
    assert(name->isNull == false);            // NOT NULL preserved
    assert(name->defaultValue.empty());       // default NOT copied
    cleanup(db);
    std::cout << "[LIKE] basic structure copy OK" << std::endl;
}

static void test_like_including_defaults() {
    std::string db = testDbPath("like_defaults");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT, label VARCHAR(20) DEFAULT 'hi')", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src INCLUDING DEFAULTS)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    const dbms::Column* label = findCol(schema, "label");
    assert(label);
    assert(!label->defaultValue.empty());     // default copied
    cleanup(db);
    std::cout << "[LIKE] INCLUDING DEFAULTS OK" << std::endl;
}

static void test_like_including_all() {
    std::string db = testDbPath("like_all");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT PRIMARY KEY, amt INT DEFAULT 5)", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src INCLUDING ALL)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    const dbms::Column* id = findCol(schema, "id");
    const dbms::Column* amt = findCol(schema, "amt");
    assert(id && amt);
    assert(id->isPrimaryKey == true);         // PK copied with INCLUDING ALL
    assert(!amt->defaultValue.empty());       // default copied
    cleanup(db);
    std::cout << "[LIKE] INCLUDING ALL OK" << std::endl;
}

static void test_like_options_are_ordered() {
    std::string db = testDbPath("like_option_order");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT PRIMARY KEY, amount INT DEFAULT 5)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE cpy (LIKE src INCLUDING ALL EXCLUDING DEFAULTS)", s));
    auto schema = g_engine.getTableSchema(db, "cpy");
    const dbms::Column* id = findCol(schema, "id");
    const dbms::Column* amount = findCol(schema, "amount");
    assert(id && amount);
    assert(id->isPrimaryKey);
    assert(amount->defaultValue.empty());

    assert(!ddl.executeSql(
        "CREATE TABLE cpy_last (LIKE src INCLUDING ALL EXCLUDING DEFAULTS "
        "INCLUDING DEFAULTS)", s));
    schema = g_engine.getTableSchema(db, "cpy_last");
    amount = findCol(schema, "amount");
    assert(amount && !amount->defaultValue.empty());
    cleanup(db);
    std::cout << "[LIKE] option ordering OK" << std::endl;
}

static void test_like_generated_is_opt_in() {
    std::string db = testDbPath("like_generated");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (a INT, doubled INT GENERATED ALWAYS AS (a * 2) STORED)", s));
    assert(!ddl.executeSql("CREATE TABLE plain (LIKE src)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE generated (LIKE src INCLUDING GENERATED)", s));

    const auto plainSchema = g_engine.getTableSchema(db, "plain");
    const auto generatedSchema = g_engine.getTableSchema(db, "generated");
    const dbms::Column* plain = findCol(plainSchema, "doubled");
    const dbms::Column* generated = findCol(generatedSchema, "doubled");
    assert(plain && plain->generatedExpr.empty() && plain->generatedKind == 0);
    assert(generated && generated->generatedExpr == "a * 2");
    assert(generated->generatedKind == 's');
    cleanup(db);
    std::cout << "[LIKE] generated columns are opt-in OK" << std::endl;
}

static void test_like_including_comments() {
    std::string db = testDbPath("like_comments");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT, note VARCHAR(20))", s));
    assert(g_engine.commentOnTable(db, "src", "source table") ==
           dbms::DBStatus::OK);
    assert(g_engine.commentOnColumn(db, "src", "note", "copied note") ==
           dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE plain (LIKE src)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE cpy (LIKE src INCLUDING COMMENTS)", s));

    assert(g_engine.getColumnComment(db, "plain", "note").empty());
    assert(g_engine.getColumnComment(db, "cpy", "note") == "copied note");
    // LIKE copies comments on copied objects, not the source relation itself.
    assert(g_engine.getTableComment(db, "cpy").empty());
    cleanup(db);
    std::cout << "[LIKE] INCLUDING COMMENTS OK" << std::endl;
}

static void test_like_invalid_options_fail_before_creation() {
    std::string db = testDbPath("like_invalid_option");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT)", s));
    assert(ddl.executeSql(
        "CREATE TABLE unknown_opt (LIKE src INCLUDING BANANAS)", s));
    assert(ddl.executeSql(
        "CREATE TABLE missing_opt (LIKE src INCLUDING)", s));
    assert(!g_engine.tableExists(db, "unknown_opt"));
    assert(!g_engine.tableExists(db, "missing_opt"));
    cleanup(db);
    std::cout << "[LIKE] invalid options rejected OK" << std::endl;
}

static void test_like_statistics_never_silently_ignored() {
    std::string db = testDbPath("like_statistics");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (a INT, b INT)", s));
    {
        std::ofstream catalog(g_engine.dbPath(db) / ".extended_stats");
        assert(catalog);
        catalog << "src_stats|src|a,b|ndistinct|-1\n";
        assert(catalog);
    }
    assert(ddl.executeSql(
        "CREATE TABLE cpy (LIKE src INCLUDING STATISTICS)", s));
    assert(!g_engine.tableExists(db, "cpy"));
    cleanup(db);
    std::cout << "[LIKE] unsupported statistics copy fails closed OK"
              << std::endl;
}

static void test_like_plus_extra_column() {
    std::string db = testDbPath("like_extra");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (a INT, b INT)", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src, c VARCHAR(10))", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    assert(schema.len == 3);
    assert(findCol(schema, "a") && findCol(schema, "b") && findCol(schema, "c"));
    cleanup(db);
    std::cout << "[LIKE] LIKE + extra column OK" << std::endl;
}

static void test_like_duplicate_columns_are_rejected() {
    std::string db = testDbPath("like_duplicate_columns");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT, payload INT)", s));
    assert(ddl.executeSql(
        "CREATE TABLE duplicate_local (LIKE src, id INT)", s));
    assert(ddl.executeSql(
        "CREATE TABLE duplicate_like (LIKE src, LIKE src)", s));
    assert(!g_engine.tableExists(db, "duplicate_local"));
    assert(!g_engine.tableExists(db, "duplicate_like"));

    cleanup(db);
    std::cout << "[LIKE] duplicate copied columns rejected OK" << std::endl;
}

static void test_like_missing_source() {
    std::string db = testDbPath("like_missing");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Source does not exist -> error (true).
    assert(ddl.executeSql("CREATE TABLE cpy (LIKE nope)", s));
    assert(!g_engine.tableExists(db, "cpy"));
    cleanup(db);
    std::cout << "[LIKE] missing source rejected OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_like_basic();
    test_like_including_defaults();
    test_like_including_all();
    test_like_options_are_ordered();
    test_like_generated_is_opt_in();
    test_like_including_comments();
    test_like_invalid_options_fail_before_creation();
    test_like_statistics_never_silently_ignored();
    test_like_plus_extra_column();
    test_like_duplicate_columns_are_rejected();
    test_like_missing_source();
    std::cout << "[LIKE] all passed" << std::endl;
    return 0;
}
