#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static dbms::TableSchema table(const std::string& name) {
    dbms::TableSchema result;
    result.tablename = name;
    result.append(dbms::makeIntColumn("id", false, 2));
    return result;
}

static void registerTable(dbms::StorageEngine& engine, const std::string& db,
                          const std::string& schema, const std::string& name) {
    auto owned = engine.catalogService().acquire(db);
    const auto* ns = owned->findNamespaceByName(schema);
    assert(ns);
    dbms::PgClassRow row;
    row.relname = name;
    row.relnamespace = ns->oid;
    row.relkind = 'r';
    row.relnatts = 1;
    const auto oid = owned->createClass(row);
    dbms::PgAttributeRow column;
    column.attrelid = oid;
    column.attname = "id";
    column.attnum = 1;
    column.atttypid = 23;
    owned->addAttribute(column);
    assert(owned->persistAll());
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "native_schema_explicit_engine";
    const auto db = testDbPath(name);
    const auto other = testDbPath(name + "_other");
    cleanupTestDb(name);
    cleanupTestDb(name + "_other");
    StorageEngine actual;
    assert(actual.createDatabase(db) == DBStatus::OK);
    assert(g_engine.createDatabase(other) == DBStatus::OK);
    Session decoy;
    decoy.currentDB = other;
    decoy.username = "unprivileged_decoy";
    decoy.permission = 0;
    auto* previous = currentSession();
    setCurrentSession(&decoy);
    struct Restore { Session* previous; ~Restore() { setCurrentSession(previous); } } restore{previous};
    const std::string ns = "Owner.Path";
    assert(actual.createSchema(db, ns) == DBStatus::OK);
    assert(!g_engine.schemaExists(other, ns));
    assert(currentSession() == &decoy && decoy.permission == 0 && decoy.currentDB == other);

    const std::string physical = ns + "__rows";
    assert(actual.createTable(db, table(physical)) == DBStatus::OK);
    // Low-level createTable remains storage-only; explicit registration is
    // a distinct owner action rather than an implicit catalog import.
    auto snapshot = actual.catalogService().metadataSnapshot(db);
    for (const auto& relation : snapshot.relations) assert(relation.relname != "rows");
    registerTable(actual, db, ns, "rows");
    assert(actual.insertRow(db, physical, {{"id", "7"}}) == DBStatus::OK);
    assert(actual.createIndex(db, physical, "id") == DBStatus::OK);
    assert(actual.createUDF(db, "bump", "x", "x+1", 'v', "sql", "int", "int",
                            false, false, ns) == DBStatus::OK);

    // A public decoded name with this physical prefix is not owned by ns.
    const std::string survivor = ns + "__public_survivor";
    assert(actual.createTable(db, table(survivor)) == DBStatus::OK);
    registerTable(actual, db, "public", survivor);
    assert(actual.insertRow(db, survivor, {{"id", "11"}}) == DBStatus::OK);

    assert(actual.dropSchema(db, ns, false) == DBStatus::INVALID_VALUE);
    assert(actual.tableExists(db, physical) && actual.schemaExists(db, ns));
    assert(actual.query(db, physical, {}, {"id"}).size() == 1);
    assert(actual.udfExists(db, "bump", ns));
    assert(!actual.inTransaction());

    assert(actual.beginTransaction(db, true) == DBStatus::OK);
    assert(actual.dropSchema(other, ns, true) == DBStatus::INVALID_VALUE);
    assert(actual.dropSchema(db, ns, true) == DBStatus::OK);
    assert(!actual.tableExists(db, physical) && !actual.udfExists(db, "bump", ns));
    assert(actual.tableExists(db, survivor));
    assert(actual.rollbackTransaction() == DBStatus::OK);
    assert(actual.tableExists(db, physical) && actual.schemaExists(db, ns));
    assert(actual.query(db, physical, {}, {"id"}).size() == 1);
    assert(actual.udfExists(db, "bump", ns));
    assert(actual.tableExists(db, survivor));
    assert(currentSession() == &decoy && decoy.permission == 0);

    assert(actual.beginTransaction(db, true) == DBStatus::OK);
    assert(actual.savepoint("owner_sp") == DBStatus::OK);
    assert(actual.dropSchema(db, ns, true) == DBStatus::OK);
    assert(actual.rollbackToSavepoint("owner_sp") == DBStatus::OK);
    assert(actual.tableExists(db, physical) && actual.udfExists(db, "bump", ns));
    assert(actual.commitTransaction() == DBStatus::OK);
    assert(actual.query(db, physical, {}, {"id"}).size() == 1);

    assert(actual.beginTransaction(db) == DBStatus::OK);
    assert(actual.setReadOnly(true));
    std::string state;
    try { (void)actual.dropSchema(db, ns, true); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "25006" && actual.tableExists(db, physical));
    assert(actual.rollbackTransaction() == DBStatus::OK);
    assert(actual.setReadOnly(false));

    assert(actual.dropSchema(db, ns, true) == DBStatus::OK);
    assert(!actual.tableExists(db, physical) && !actual.schemaExists(db, ns));
    assert(!actual.udfExists(db, "bump", ns));
    assert(actual.tableExists(db, survivor));
    assert(actual.query(db, survivor, {}, {"id"}).size() == 1);
    assert(!g_engine.schemaExists(other, ns));
    assert(currentSession() == &decoy && decoy.permission == 0 && decoy.currentDB == other);
    assert(actual.dropDatabase(db) == DBStatus::OK);
    assert(g_engine.dropDatabase(other) == DBStatus::OK);
    cleanupTestDb(name);
    cleanupTestDb(name + "_other");
    std::cout << "[NATIVE SCHEMA EXPLICIT ENGINE] namespace/physical/catalog/rollback owner controls passed\n";
}
