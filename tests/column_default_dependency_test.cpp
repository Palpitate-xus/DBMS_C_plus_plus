#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks; if (!value) throw std::runtime_error(reason);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("column_default_dependency");
        cleanupTestDb("column_default_dependency");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual isolated database");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = database;
        struct Scope { Session* prior = currentSession(); Scope(Session& s) { setCurrentSession(&s); }
            ~Scope() { setCurrentSession(prior); } } scope(session);
        DdlExecutor executor;
        const auto ddl = [&](const std::string& sql, const std::string& state = "") {
            std::ostringstream output;
            auto* original = std::cout.rdbuf(output.rdbuf());
            bool error = false;
            try { error = executor.executeSql(sql, session); }
            catch (...) { std::cout.rdbuf(original); throw; }
            std::cout.rdbuf(original);
            need(error == !state.empty() && (state.empty() || output.str().find(state) != std::string::npos),
                 "actual DDL status " + sql + ": " + output.str());
        };
        const auto rows = [&](const std::string& sql, const std::vector<std::vector<std::string>>& expected) {
            auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, sql));
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
                &g_engine, database, prepared, prepared->ast.get()));
            result.throwIfFailed();
            need(result.structuredRows == expected, "whole actual query rows: " + sql);
        };
        const auto insert = [&](const std::string& sql, const std::vector<std::vector<std::string>>& expected) {
            bool handled = false; clearLastDmlResult();
            need(!tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql) && handled, "actual DML: " + sql);
            need(takeLastDmlResult().rows == expected, "whole actual RETURNING rows: " + sql);
        };
        ddl("CREATE TABLE default_target(k integer)");
        ddl("CREATE TABLE default_owner(r regclass DEFAULT 'default_target'::regclass)");
        auto ownerCatalog = g_engine.catalogService().acquire(database);
        const auto target = ownerCatalog->getClassByName("default_target", 2200);
        const auto owner = ownerCatalog->getClassByName("default_owner", 2200);
        need(target && owner, "actual relation identities");
        auto definition = ownerCatalog->getAttrdefForColumn(owner->oid, 1);
        need(definition && definition->oid && definition->oid != owner->oid, "independent actual default object");
        const Oid firstDefault = definition->oid;
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='default_owner'::regclass", {{"1"}});
        rows("SELECT deptype FROM pg_depend WHERE classid=2604 AND objid=" + std::to_string(firstDefault) +
             " AND refclassid=1259 AND refobjid=" + std::to_string(target->oid), {{"n"}});
        ddl("ALTER TABLE default_target RENAME TO renamed_target");
        ddl("CREATE TABLE default_target(k integer)");
        insert("INSERT INTO default_owner DEFAULT VALUES RETURNING r::text,r='renamed_target'::regclass", {{"renamed_target", "t"}});
        ddl("DROP TABLE renamed_target", "2BP01");
        ddl("ALTER TABLE default_owner ALTER COLUMN r SET DEFAULT 'default_target'::regclass");
        ownerCatalog = g_engine.catalogService().acquire(database);
        need(ownerCatalog->getAttrdefForColumn(owner->oid, 1)->oid != firstDefault && !ownerCatalog->getAttrdef(firstDefault),
             "explicit replacement gives its own identity and removes the prior object");
        ddl("DROP TABLE renamed_target");
        ddl("DROP TABLE default_target CASCADE");
        ownerCatalog = g_engine.catalogService().acquire(database);
        need(ownerCatalog->getClass(owner->oid) && !ownerCatalog->getAttrdefForColumn(owner->oid, 1) &&
             !ownerCatalog->getAttribute(owner->oid, "r")->atthasdef, "CASCADE preserves owner and column, removes only the default");
        insert("INSERT INTO default_owner DEFAULT VALUES RETURNING r IS NULL", {{"t"}});

        ddl("CREATE TABLE self_default(r regclass DEFAULT 'self_default'::regclass)");
        insert("INSERT INTO self_default DEFAULT VALUES RETURNING r::text", {{"self_default"}});
        ddl("DROP TABLE self_default");
        ddl("CREATE TABLE bad_default(r regclass DEFAULT 'no_such_default_target'::regclass)", "42P01");
        rows("SELECT oid FROM pg_class WHERE relname='bad_default'", {});
        ddl("CREATE TEMP TABLE bad_temp_default(r regclass DEFAULT 'no_such_default_target'::regclass)", "42P01");
        need(!session.tempTables.count("bad_temp_default"), "failed DEFAULT declaration does not register a temporary table");

        ddl("CREATE TABLE null_defaults(a regclass DEFAULT NULL,b regclass DEFAULT NULL::regclass,c integer DEFAULT NULL,d text DEFAULT 'x')");
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='null_defaults'::regclass ORDER BY adnum", {{"4"}});
        ddl("ALTER TABLE null_defaults ALTER COLUMN a SET DEFAULT NULL::regclass");
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='null_defaults'::regclass ORDER BY adnum", {{"4"}});
        ddl("CREATE TYPE referenced_enum AS ENUM ('a','b')");
        ddl("CREATE TABLE enum_default_owner(r regtype DEFAULT 'referenced_enum'::regtype)");
        ddl("DROP TYPE referenced_enum", "2BP01");
        ddl("DROP TYPE referenced_enum CASCADE");
        insert("INSERT INTO enum_default_owner DEFAULT VALUES RETURNING r IS NULL", {{"t"}});

        ddl("CREATE TABLE serial_default(id serial, kept integer)");
        ddl("CREATE TABLE external_default(id integer DEFAULT nextval('serial_default_id_seq'))");
        ddl("ALTER TABLE serial_default DROP COLUMN id", "2BP01");
        rows("SELECT attname FROM pg_attribute WHERE attrelid='serial_default'::regclass AND attnum>0 ORDER BY attnum", {{"id"}, {"kept"}});
        ddl("ALTER TABLE serial_default DROP COLUMN id CASCADE");
        need(!g_engine.sequenceExists(database, "serial_default_id_seq"), "owning column drop removes its actual owned sequence");
        insert("INSERT INTO external_default DEFAULT VALUES RETURNING id IS NULL", {{"t"}});
        ddl("CREATE TABLE serial_restrict(id serial, kept integer)");
        ddl("ALTER TABLE serial_restrict DROP COLUMN id");
        need(!g_engine.sequenceExists(database, "serial_restrict_id_seq"), "owning default is a pending root during RESTRICT");

        ddl("CREATE TABLE cascade_atomic_target(k integer)");
        ddl("ALTER TABLE default_owner ALTER COLUMN r SET DEFAULT 'cascade_atomic_target'::regclass");
        need(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual DDL transaction");
        need(g_engine.savepoint("before_default_drop") == DBStatus::OK, "actual default dependency savepoint");
        ddl("DROP TABLE cascade_atomic_target CASCADE");
        need(g_engine.rollbackToSavepoint("before_default_drop") == DBStatus::OK, "actual physical and catalog savepoint restore");
        ddl("DROP TABLE cascade_atomic_target", "2BP01");
        insert("INSERT INTO default_owner DEFAULT VALUES RETURNING r::text", {{"cascade_atomic_target"}});
        need(g_engine.rollbackTransaction() == DBStatus::OK, "actual outer rollback");
        rows("SELECT adnum FROM pg_attrdef WHERE adrelid='default_owner'::regclass", {{"1"}});
        need(g_engine.checkpoint(database), "actual checkpoint before independent cold catalog load");
        const auto cold = CatalogManager::readMetadataSnapshot((g_engine.dbPath(database) / "pg_catalog").string());
        need(std::any_of(cold.defaults.begin(), cold.defaults.end(), [&](const auto& d) { return d.adrelid == owner->oid; }),
             "cold catalog contains the surviving actual default object");
        std::cout << "COLUMN_DEFAULT_DEPENDENCY_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "COLUMN_DEFAULT_DEPENDENCY_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}
