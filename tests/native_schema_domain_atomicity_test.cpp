#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "commands/SchemaNamespace.h"
#include "catalog/CatalogService.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto name = "schema_domain_atomicity";
    const auto database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "domain_namespace_owner";
    session.permission = 1;
    session.currentDB = database;
    session.searchPath = "public";
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA domain_chain", session));
    assert(!ddl.executeSql("CREATE DOMAIN domain_chain.parent_int AS INT CHECK (VALUE > 0)", session));
    assert(!ddl.executeSql("CREATE DOMAIN public.child_int AS domain_chain.parent_int", session));
    assert(!ddl.executeSql("CREATE TABLE public.\"domain_chain__survivor\"(id INT PRIMARY KEY, first_value domain_chain.parent_int, second_value public.child_int, wide BIGINT, note TEXT)", session));
    const std::string physical = "domain_chain__survivor";
    assert(g_engine.insertRow(database, physical, {{"id", "1"}, {"first_value", std::nullopt}, {"second_value", "7"}, {"wide", "2147483648"}, {"note", ""}}) == DBStatus::OK);
    const auto before = g_engine.query(database, physical, {}, {"id", "second_value", "wide"});
    assert(before == std::vector<std::string>{"1 7 2147483648 "});
    // The low-level table primitive deliberately does not register pg_class.
    // Its real domain dependency still must not become a dangling sidecar.
    TableSchema raw;
    raw.tablename = "raw_domain_external";
    raw.append(makeIntColumn("id", true, 2, true));
    auto rawDomain = g_engine.getTableSchema(database, physical).cols[1];
    // Retain the already-bound column name along with its CHECK expression.
    raw.append(rawDomain);
    auto rawNote = g_engine.getTableSchema(database, physical).cols[4];
    rawNote.dataName = "raw_note"; raw.append(rawNote);
    assert(g_engine.createTable(database, raw) == DBStatus::OK);
    assert(g_engine.insertRow(database, raw.tablename, {{"id", "2"}, {"first_value", "8"}, {"raw_note", std::nullopt}}) == DBStatus::OK);
    assert(!g_engine.catalogService().get(database).resolveRelation(raw.tablename, {"public"}));
    const auto unchanged = [&] {
        const auto table = g_engine.getTableSchema(database, physical);
        assert(table.len == 5);
        assert(g_engine.schemaExists(database, "domain_chain"));
        assert(g_engine.query(database, physical, {}, {"id", "second_value", "wide"}) == before);
        assert(!g_engine.getDomain(database, "public.child_int").name.empty());
        size_t rows = 0;
        assert(g_engine.forEachRow(database, physical, [&](uint32_t page, uint16_t slot, const char*, size_t) {
            const auto rid = StorageEngine::encodeRid(page, slot);
            assert(g_engine.isColumnNullByRid(database, physical, rid, 1));
            assert(!g_engine.isColumnNullByRid(database, physical, rid, 4));
            ++rows;
        }));
        assert(rows == 1);
        assert(g_engine.getTableSchema(database, raw.tablename).len == 3);
        assert(g_engine.query(database, raw.tablename, {}, {"id", "first_value"}) == std::vector<std::string>{"2 8 "});
    };
    unchanged();
    assert(g_engine.dropSchema(database, "domain_chain", false) == DBStatus::INVALID_VALUE);
    unchanged();
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.dropSchema(database, "domain_chain", true) == DBStatus::OK);
    assert(g_engine.getTableSchema(database, physical).len == 3);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    unchanged();
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.savepoint("domain_namespace") == DBStatus::OK);
#ifdef DBMS_NAMESPACE_DIAGNOSTIC
    const auto diagnostic = dropSchemaNamespace(g_engine, database, "domain_chain", true);
    std::cout << "[SCHEMA DOMAIN DIAGNOSTIC] status=" << static_cast<int>(diagnostic.status)
              << " error=" << diagnostic.error << '\n' << std::flush;
    assert(diagnostic.status == DBStatus::OK);
#else
    assert(g_engine.dropSchema(database, "domain_chain", true) == DBStatus::OK);
#endif
    assert(g_engine.rollbackToSavepoint("domain_namespace") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    unchanged();
    assert(g_engine.dropSchema(database, "domain_chain", true) == DBStatus::OK);
    const auto after = g_engine.getTableSchema(database, physical);
    assert(after.len == 3 && after.cols[0].dataName == "id" &&
           after.cols[1].dataName == "wide" && after.cols[2].dataName == "note");
    assert(g_engine.query(database, physical, {}, {"id", "wide"}) == std::vector<std::string>{"1 2147483648 "});
    assert(g_engine.getTableSchema(database, raw.tablename).len == 2);
    assert(g_engine.query(database, raw.tablename, {}, {"id"}) == std::vector<std::string>{"2 "});
    assert(g_engine.getDomain(database, "public.child_int").name.empty());
    assert(!g_engine.schemaExists(database, "domain_chain"));
    {
        StorageEngine restarted;
        assert(restarted.getTableSchema(database, physical).len == 3);
        assert(restarted.query(database, physical, {}, {"id", "wide"}) == std::vector<std::string>{"1 2147483648 "});
        assert(restarted.getTableSchema(database, raw.tablename).len == 2);
        assert(restarted.query(database, raw.tablename, {}, {"id"}) == std::vector<std::string>{"2 "});
        const auto lease = restarted.catalogService().acquire(database);
        const auto* publicNamespace = lease->findNamespaceByName("public");
        assert(publicNamespace && lease->findClassByName(physical, publicNamespace->oid));
        assert(!lease->findNamespaceByName("domain_chain"));
        assert(!lease->findTypeByName("child_int", publicNamespace->oid));
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[SCHEMA DOMAIN ATOMICITY] real descendant/quoted physical identity/NULL/outer rollback/savepoint/commit/restart passed\n";
}
