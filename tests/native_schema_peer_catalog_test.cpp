#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

static bool has(dbms::StorageEngine& engine, const std::string& database,
                const std::string& schema) {
    for (const auto& row : engine.catalogService().metadataSnapshot(database).namespaces)
        if (row.nspname == schema) return true;
    return false;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="native_schema_peer_catalog", db=testDbPath(name);
    cleanupTestDb(name);
    // Engine startup performs recovery. Construct every engine before any
    // parent transaction; "cold" below means its catalog cache is cold.
    StorageEngine writer, reader, cold;
    assert(writer.createDatabase(db) == DBStatus::OK);
    assert(writer.createSchema(db,"owned") == DBStatus::OK);
    assert(writer.catalogService().get(db).findNamespaceByName("owned"));
    assert(reader.catalogService().get(db).findNamespaceByName("owned"));
    assert(writer.dropSchema(db,"owned",false) == DBStatus::OK);
    assert(!writer.schemaExists(db,"owned"));
    assert(!has(writer,db,"owned"));
    // A second actual engine that already loaded this physical database must
    // not retain the dropped namespace or republish it during its next commit.
    assert(!has(reader,db,"owned"));
    assert(!reader.catalogService().get(db).findNamespaceByName("owned"));
    assert(reader.beginTransaction(db) == DBStatus::OK);
    assert(reader.commitTransaction() == DBStatus::OK);
    assert(!has(reader,db,"owned"));
    assert(reader.createSchema(db,"replacement") == DBStatus::OK);
    assert(has(writer,db,"replacement"));

    // Each engine owns its model. A durable physical DDL publication while
    // its parent transaction remains active must not be visible to peers.
    assert(writer.createSchema(db,"committed_before") == DBStatus::OK);
    (void)reader.catalogService().get(db);
    const auto oldLease = reader.catalogService().acquire(db);
    const auto* oldRow = oldLease->findNamespaceByName("committed_before");
    assert(oldRow);
    const auto oldOid = oldRow->oid;
    assert(writer.beginTransaction(db, true) == DBStatus::OK);
    assert(writer.dropSchema(db,"committed_before",false) == DBStatus::OK);
    assert(writer.createSchema(db,"pending_commit") == DBStatus::OK);
    assert(!has(writer,db,"committed_before") && has(writer,db,"pending_commit"));
    assert(has(reader,db,"committed_before") && !has(reader,db,"pending_commit"));
    assert(reader.catalogService().get(db).findNamespaceByName("committed_before"));
    assert(!reader.catalogService().get(db).findNamespaceByName("pending_commit"));
    // A cold reader currently fails closed rather than reading uncommitted
    // CSV files. PostgreSQL can return an older MVCC image here (OPEN).
    std::string coldState;
    try { (void)cold.catalogService().metadataSnapshot(db); }
    catch (const DbError& error) { coldState = error.sqlState(); }
    assert(coldState == "55P03");
    assert(writer.commitTransaction() == DBStatus::OK);
    assert(!has(reader,db,"committed_before") && has(reader,db,"pending_commit"));
    assert(!reader.catalogService().get(db).findNamespaceByName("committed_before"));
    assert(reader.catalogService().get(db).findNamespaceByName("pending_commit"));
    assert(oldRow->oid == oldOid && oldRow->nspname == "committed_before");
    assert(oldLease->findNamespaceByName("committed_before") == oldRow);

    // Implicit service publication during BEGIN must likewise use a new
    // generation instead of invalidating a retained model's row pointers.
    const auto beforeBegin = reader.catalogService().acquire(db);
    const auto* beforeBeginRow = beforeBegin->findNamespaceByName("pending_commit");
    assert(beforeBeginRow);
    const auto beforeBeginOid = beforeBeginRow->oid;
    assert(writer.dropSchema(db,"pending_commit",false) == DBStatus::OK);
    assert(reader.beginTransaction(db) == DBStatus::OK);
    assert(reader.commitTransaction() == DBStatus::OK);
    assert(!has(reader,db,"pending_commit"));
    assert(beforeBeginRow->oid == beforeBeginOid && beforeBeginRow->nspname == "pending_commit");

    assert(writer.createSchema(db,"rollback_original") == DBStatus::OK);
    (void)reader.catalogService().get(db);
    const auto rollbackLease = reader.catalogService().acquire(db);
    const auto* rollbackRow = rollbackLease->findNamespaceByName("rollback_original");
    assert(rollbackRow);
    const auto rollbackOid = rollbackRow->oid;
    // A lexical alias of the same physical directory has the same owner.
    const auto alias = db + "/.";
    assert(reader.catalogService().get(alias).findNamespaceByName("rollback_original"));
    assert(writer.beginTransaction(db, true) == DBStatus::OK);
    assert(writer.dropSchema(db,"rollback_original",false) == DBStatus::OK);
    assert(writer.createSchema(db,"rolled_back") == DBStatus::OK);
    assert(has(reader,db,"rollback_original") && !has(reader,db,"rolled_back"));
    assert(has(reader,alias,"rollback_original") && !has(reader,alias,"rolled_back"));
    assert(writer.rollbackTransaction() == DBStatus::OK);
    assert(has(writer,db,"rollback_original") && !has(writer,db,"rolled_back"));
    assert(has(reader,db,"rollback_original") && !has(reader,db,"rolled_back"));
    assert(reader.catalogService().get(db).findNamespaceByName("rollback_original"));
    assert(!reader.catalogService().get(db).findNamespaceByName("rolled_back"));
    assert(rollbackRow->oid == rollbackOid && rollbackRow->nspname == "rollback_original");

    assert(writer.beginTransaction(db, true) == DBStatus::OK);
    assert(writer.savepoint("namespace_sp") == DBStatus::OK);
    assert(writer.dropSchema(db,"rollback_original",false) == DBStatus::OK);
    assert(writer.createSchema(db,"savepoint_only") == DBStatus::OK);
    assert(writer.rollbackToSavepoint("namespace_sp") == DBStatus::OK);
    assert(has(writer,db,"rollback_original") && !has(writer,db,"savepoint_only"));
    assert(writer.commitTransaction() == DBStatus::OK);
    assert(has(reader,db,"rollback_original") && !has(reader,db,"savepoint_only"));
    assert(rollbackRow->oid == rollbackOid && rollbackRow->nspname == "rollback_original");

    // Refresh is never permission to discard unpublished edits. Real raw
    // catalog mutation plus a peer publication is an explicit conflict.
    const auto dirty = reader.catalogService().acquire(db);
    const auto dirtyOid = dirty->createNamespace("unpublished_local", INVALID_OID);
    assert(writer.createSchema(db,"peer_published") == DBStatus::OK);
    std::string dirtyState;
    try { (void)reader.catalogService().get(db); }
    catch (const DbError& error) { dirtyState = error.sqlState(); }
    assert(dirtyState == "58030");
    assert(!reader.catalogService().persistAll());
    assert(dirty->findNamespaceByName("unpublished_local"));
    assert(has(writer,db,"peer_published"));
    std::string dropError;
    assert(dirty->dropObject(PgClassOid_Namespace, dirtyOid,
                            CatalogManager::DropBehavior::Restrict, &dropError));
    assert(reader.catalogService().get(db).findNamespaceByName("peer_published"));

    assert(writer.dropDatabase(db) == DBStatus::OK);
    assert(writer.createDatabase(db) == DBStatus::OK);
    assert(writer.createSchema(db,"new_database_generation") == DBStatus::OK);
    assert(!has(reader,db,"rollback_original"));
    assert(has(reader,db,"new_database_generation"));
    assert(!reader.catalogService().get(db).findNamespaceByName("rollback_original"));
    assert(reader.catalogService().get(db).findNamespaceByName("new_database_generation"));
    // Eviction/physical replacement does not destroy a retained generation.
    reader.catalogService().evict(db);
    assert(rollbackRow->oid == rollbackOid && rollbackRow->nspname == "rollback_original");
    assert(writer.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    {
        const auto lifetimeName = name + "_lease_lifetime";
        const auto lifetimeDb = testDbPath(lifetimeName);
        cleanupTestDb(lifetimeName);
        std::shared_ptr<CatalogManager> lifetimeLease;
        const PgNamespaceRow* lifetimeRow = nullptr;
        Oid lifetimeOid = INVALID_OID;
        {
            StorageEngine temporaryOwner;
            assert(temporaryOwner.createDatabase(lifetimeDb) == DBStatus::OK);
            assert(temporaryOwner.createSchema(lifetimeDb,"leased") == DBStatus::OK);
            (void)temporaryOwner.catalogService().get(lifetimeDb);
            lifetimeLease = temporaryOwner.catalogService().acquire(lifetimeDb);
            lifetimeRow = lifetimeLease->findNamespaceByName("leased");
            assert(lifetimeRow);
            lifetimeOid = lifetimeRow->oid;
        }
        // The explicit lease owns only its immutable generation's model,
        // never a callback into an engine that has already been destroyed.
        assert(lifetimeRow->oid == lifetimeOid && lifetimeRow->nspname == "leased");
        StorageEngine replacementOwner;
        assert(replacementOwner.dropDatabase(lifetimeDb) == DBStatus::OK);
        lifetimeLease.reset();
        assert(!std::filesystem::exists(lifetimeDb));
        cleanupTestDb(lifetimeName);
    }
    std::cout<<"[NATIVE SCHEMA PEER] canonical live catalog ownership and peer commit passed\n";
}
