#include "commands/DdlExecutor.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "permissions.h"
#include <cassert>
#include <filesystem>
#include <iostream>
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

static void test_bridge_handles_create_table() {
    std::string db = testDbPath("ddl_route_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    std::string sql = "create table rt1 (id integer primary key, name varchar(100))";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    bool handled = false;
    bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(handled);
    assert(!err);
    assert(g_engine.tableExists(db, "rt1"));

    // Running the same CREATE again must error exactly once (no double-exec bug).
    err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(handled);
    assert(err);
    assert(g_engine.tableExists(db, "rt1"));

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge handles CREATE TABLE and single-runs it OK" << std::endl;
}

static void test_bridge_falls_back_for_unhandled() {
    std::string db = testDbPath("ddl_route_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    // CREATE CAST is not in the bridge set.
    std::string sql = "create cast (int as text) with function int2text";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    bool handled = false;
    bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(!handled);
    assert(!err);

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge falls back for unhandled DDL OK" << std::endl;
}

static void test_bridge_fails_closed_on_parse_error() {
    Session s;
    setupSession(s, "");

    // The caller normally supplies classify(sql); use an explicit bridge-owned
    // command here to exercise the parse-error contract independently.
    bool handled = false;
    bool err = dbms::tryDdlBridge("", dbms::SqlCommand::CreateTable, s, handled);
    assert(handled);
    assert(err);

    std::cout << "[DDL-ROUTE] parse errors fail closed" << std::endl;
}

static void test_unknown_column_type_fails_closed() {
    const std::string db = testDbPath("ddl_route_unknown_type");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql("CREATE TABLE bad_type (id definitely_not_a_type)", s));
    assert(!g_engine.tableExists(db, "bad_type"));

    assert(!ddl.executeSql("CREATE TABLE good_type (id INT)", s));
    assert(ddl.executeSql("ALTER TABLE good_type ADD COLUMN broken definitely_not_a_type", s));
    const auto schema = g_engine.getTableSchema(db, "good_type");
    assert(schema.len == 1);
    assert(schema.cols[0].dataName == "id");

    cleanup(db);
    std::cout << "[DDL-ROUTE] unknown column types fail closed" << std::endl;
}

static void test_bridge_handles_domain_sequence_schema() {
    const std::string db = testDbPath("ddl_route_objects");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    auto run = [&](const std::string& sql) {
        bool handled = false;
        const auto command = dbms::SQLParser::classify(sql);
        const bool error = dbms::tryDdlBridge(sql, command, s, handled);
        assert(handled);
        assert(!error);
    };

    run("CREATE DOMAIN route_text AS VARCHAR(12) CHECK (length(VALUE) <= 12)");
    const auto domain = g_engine.getDomain(db, "route_text");
    // SQL DDL retains the declaration's real quoted namespace identity.
    // Keep the original unqualified SQL and lookup, not a different input.
    assert(domain.name == "\"public\".\"route_text\"");
    dbms::CatalogManager::QualifiedName identity;
    assert(dbms::CatalogManager::parseQualifiedName(domain.name, identity));
    assert(identity.schema == "public" && identity.name == "route_text");
    assert(g_engine.getDomain(db, domain.name).name == domain.name);
    // The stored source can contain token whitespace. Retain the complete
    // original expected type and modifier semantics instead of print style.
    const auto actualBase = dbms::SQLParser::parseTypeSpecification(domain.baseType);
    const auto originalBase = dbms::SQLParser::parseTypeSpecification("VARCHAR(12)");
    assert(actualBase.typeName == originalBase.typeName);
    assert(actualBase.typeMods == originalBase.typeMods);
    assert(actualBase.isArray == originalBase.isArray);
    assert(actualBase.typeMods == std::vector<std::string>{"12"});
    assert(!actualBase.isArray);
    auto& catalog = g_engine.catalogService().get(db);
    const auto* namespaceRow = catalog.findNamespaceByName(identity.schema);
    assert(namespaceRow);
    const dbms::Oid namespaceOid = namespaceRow->oid;
    const auto* domainType = catalog.findTypeByName(identity.name, namespaceOid);
    assert(domainType && domainType->typtype == 'd');
    assert(domainType->typnamespace == namespaceOid);
    assert(domainType->typtypmod == 16);
    const auto* baseType = catalog.findType(domainType->typbasetype);
    assert(baseType && baseType->typname == "varchar");

    assert(dbms::SQLParser::classify("CREATE SEQUENCE route_seq") ==
           dbms::SqlCommand::CreateSequence);
    run("CREATE SEQUENCE route_seq START 7 INCREMENT 2");
    assert(g_engine.sequenceExists(db, "route_seq"));

    assert(dbms::SQLParser::classify("ALTER SEQUENCE route_seq RESTART") ==
           dbms::SqlCommand::AlterSequence);
    run("ALTER SEQUENCE route_seq RESTART WITH 20 INCREMENT BY 3");
    assert(g_engine.nextval(db, "route_seq") == 20);
    assert(g_engine.nextval(db, "route_seq") == 23);

    run("CREATE SCHEMA route_schema");
    assert(g_engine.schemaExists(db, "route_schema"));

    run("DROP DOMAIN route_text");
    assert(g_engine.getDomain(db, domain.name).name.empty());
    assert(!g_engine.catalogService().get(db).findTypeByName(identity.name,
                                                             namespaceOid));
    run("DROP SEQUENCE route_seq");
    run("DROP SCHEMA route_schema");
    assert(g_engine.getDomain(db, "route_text").name.empty());
    assert(!g_engine.sequenceExists(db, "route_seq"));
    assert(!g_engine.schemaExists(db, "route_schema"));

    cleanup(db);
    std::cout << "[DDL-ROUTE] domain/sequence/schema use typed bridge" << std::endl;
}

static void test_supported_serial_type_mapping() {
    const std::string db = testDbPath("ddl_route_serial");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    // NCHAR/BINARY are extended-mode aliases (DIV-06).
    s.compatibilityMode = "extended";
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE serial_t (id SERIAL PRIMARY KEY, label NCHAR(4), raw BINARY(4))", s));

    const auto schema = g_engine.getTableSchema(db, "serial_t");
    assert(schema.len == 3);
    // SERIAL is now an owned named sequence and a nextval DEFAULT, not the
    // legacy anonymous auto-increment counter (nor an IDENTITY column).
    assert(!schema.cols[0].isAutoIncrement);
    assert(!schema.cols[0].isNull && schema.cols[0].identityKind == 0);
    assert(!schema.cols[0].defaultValue.empty());
    dbms::SequenceInfo sequence;
    assert(g_engine.getSequenceInfo(db, "serial_t_id_seq", sequence) == dbms::DBStatus::OK);
    assert(sequence.ownedByTable == "serial_t" && sequence.ownedByColumn == "id");
    assert(schema.cols[1].dataType == "char");
    assert(schema.cols[2].dataType == "binary");
    assert(g_engine.insert(db, "serial_t", {{"label", "a"}, {"raw", "0102"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "serial_t", {{"label", "b"}, {"raw", "0304"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(db, "serial_t", {}, {"id"}) ==
           (std::vector<std::string>{"1 ", "2 "}));
    assert(g_engine.currval(db, "serial_t_id_seq") == 2);

    cleanup(db);
    std::cout << "[DDL-ROUTE] supported SERIAL/type mappings OK" << std::endl;
}

static void test_bridge_handles_ctas() {
    std::string db = testDbPath("ddl_route_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    // First create a source table.
    std::string src = "create table src (id int)";
    dbms::SqlCommand srcCmd = dbms::SQLParser::classify(src);
    bool handled = false;
    bool err = dbms::tryDdlBridge(src, srcCmd, s, handled);
    assert(handled && !err);

    std::string sql = "create table ctas_dst as select * from src";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    err = dbms::tryDdlBridge(sql, cmd, s, handled);
    // CTAS is now handled by the DDL AST bridge.
    assert(handled);
    assert(!err);
    assert(g_engine.tableExists(db, "ctas_dst"));

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge handles CTAS OK" << std::endl;
}

static void test_bridge_handles_typed_alter_table() {
    std::string db = testDbPath("ddl_route_alter");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    auto run = [&](const std::string& sql) {
        bool handled = false;
        auto cmd = dbms::SQLParser::classify(sql);
        bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
        assert(handled);
        assert(!err);
    };

    run("CREATE TABLE alter_t (id INT, name VARCHAR(20))");
    run("ALTER TABLE alter_t ADD COLUMN score INT");
    run("ALTER TABLE alter_t ADD COLUMN IF NOT EXISTS score INT");
    assert(g_engine.getTableSchema(db, "alter_t").len == 3);
    run("ALTER TABLE alter_t ALTER COLUMN score SET DEFAULT 10");
    run("ALTER TABLE alter_t ALTER COLUMN score SET NOT NULL");
    run("ALTER TABLE alter_t ADD PRIMARY KEY (id)");
    run("ALTER TABLE alter_t DROP CONSTRAINT IF EXISTS alter_t_pkey");
    run("ALTER TABLE alter_t DROP CONSTRAINT IF EXISTS missing_constraint");
    run("ALTER TABLE alter_t RENAME COLUMN name TO full_name");
    auto renamed = g_engine.getTableSchema(db, "alter_t");
    assert(renamed.cols[1].dataName == "full_name");
    run("ALTER TABLE alter_t RENAME TO alter_t_v2");
    assert(g_engine.tableExists(db, "alter_t_v2"));

    dbms::SQLParser parser;
    auto parsed = parser.parse("ALTER TABLE alter_t_v2 RENAME TO alter_t_v3");
    assert(parsed.success);
    auto* alter = dynamic_cast<dbms::AlterTableStmt*>(parsed.stmt.get());
    assert(alter && alter->subCommands.size() == 1);
    assert(alter->subCommands[0].action == dbms::AlterTableStmt::Action::RenameTable);

    cleanup(db);
    std::cout << "[DDL-ROUTE] typed ALTER TABLE bridge OK" << std::endl;
}

static void test_comment_uses_raw_literal_text() {
    const std::string db = testDbPath("ddl_route_comment_text");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);

    dbms::TableSchema table;
    table.tablename = "notes";
    table.append(dbms::makeIntColumn("id", false, 0, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    const std::string raw =
        "CoMmEnT ON TABLE notes IS 'Mixed Case\nLine ''quoted'' | pipe'";
    // executeInternal supplies the bridge a normalized string whose physical
    // newlines have already been removed, plus the original SQL separately.
    const std::string normalized =
        "comment on table notes is 'Mixed CaseLine ''quoted'' | pipe'";
    bool handled = false;
    const bool error = dbms::tryDdlBridge(
        normalized, dbms::SqlCommand::Comment, s, handled, raw);
    assert(handled);
    assert(!error);
    assert(g_engine.getTableComment(db, "notes") ==
           "Mixed Case\nLine 'quoted' | pipe");

    cleanup(db);
    std::cout << "[DDL-ROUTE] COMMENT raw literal preservation OK" << std::endl;
}

static void test_bridge_handles_catalog_auth_ddl() {
    Session s;
    setupSession(s, "");
    const std::string userName = "route_auth_user";
    const std::string roleName = "route_auth_role";
    const std::string renamedRole = "route_auth_role_renamed";
    dropRole(userName);
    dropRole(roleName);
    dropRole(renamedRole);

    auto run = [&](const std::string& sql) {
        bool handled = false;
        const auto cmd = dbms::SQLParser::classify(sql);
        const bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
        assert(handled);
        assert(!err);
    };

    run("CREATE USER Route_Auth_User WITH PASSWORD 'MiXeD-Case-9!' LOGIN");
    auto account = authCatalog().getAuthIdByName(userName);
    assert(account && account->rolcanlogin);
    assert(verifyUserPassword(userName, "MiXeD-Case-9!"));

    run("ALTER USER Route_Auth_User WITH NOLOGIN PASSWORD 'SeCoNd-Secret-8!'");
    account = authCatalog().getAuthIdByName(userName);
    assert(account && !account->rolcanlogin);
    run("ALTER USER Route_Auth_User WITH LOGIN");
    assert(verifyUserPassword(userName, "SeCoNd-Secret-8!"));

    run("CREATE ROLE Route_Auth_Role");
    run("ALTER ROLE Route_Auth_Role RENAME TO Route_Auth_Role_Renamed");
    assert(!roleExists(roleName));
    assert(roleExists(renamedRole));
    run("DROP ROLE Route_Auth_Role_Renamed");
    run("DROP USER Route_Auth_User");
    assert(!roleExists(userName));

    std::cout << "[DDL-ROUTE] catalog auth DDL and literal case preservation OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bridge_handles_create_table();
    test_bridge_falls_back_for_unhandled();
    test_bridge_fails_closed_on_parse_error();
    test_unknown_column_type_fails_closed();
    test_bridge_handles_domain_sequence_schema();
    test_supported_serial_type_mapping();
    test_bridge_handles_ctas();
    test_bridge_handles_typed_alter_table();
    test_comment_uses_raw_literal_text();
    test_bridge_handles_catalog_auth_ddl();
    std::cout << "[DDL-ROUTE] all passed" << std::endl;
    return 0;
}
