#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>

namespace fs = std::filesystem;
using namespace dbms;

static struct stat physicalIdentity(const fs::path& path) {
    struct stat result{};
    assert(::stat(path.c_str(), &result) == 0);
    return result;
}

static bool sameFile(const struct stat& first, const struct stat& second) {
    return first.st_dev == second.st_dev && first.st_ino == second.st_ino;
}

int main() {
    const std::string db = "savepoint_image_demand_db";
    StorageEngine engine;
    assert(engine.createDatabase(db) == DBStatus::OK);
    assert(engine.beginTransaction(db, true) == DBStatus::OK);
    assert(engine.createTransactionBackup());
    engine.preserveTransactionBackupOnRollback(true);
    engine.restoreTransactionBackupBeforeRowUndo(true);
    engine.markTransactionBackupDirty();
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, true));
    table.pkColIndices.push_back(0);
    assert(engine.createTable(db, table) == DBStatus::OK);
    assert(engine.insert(db, "items", {{"id", "7"}}) == DBStatus::OK);
    auto& initialCatalog = engine.catalogService().get(db);
    const auto* initialNamespace = initialCatalog.findNamespaceByName("public");
    assert(initialNamespace);
    PgClassRow initialRelation;
    initialRelation.relname = "items";
    initialRelation.relnamespace = initialNamespace->oid;
    const auto relationId = initialCatalog.createClass(initialRelation);
    assert(relationId != 0);
    assert(engine.savepoint("read_only_image") == DBStatus::OK);
    const auto schemaPath = engine.dbPath(db) / "items.stc";
    const auto saved = physicalIdentity(schemaPath);
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);
    assert(engine.rollbackToSavepoint("read_only_image") == DBStatus::OK);
    const auto after = physicalIdentity(schemaPath);
    if (!sameFile(saved, after)) {
        std::cerr << "unchanged savepoint restored the entire database image\n";
        assert(false);
    }
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);

    // Mutable catalog rows are public API: no logical mutation epoch changes.
    // The original full restore must still restore their persisted preimage.
    const auto* relation = engine.catalogService().get(db).findClass(relationId);
    assert(relation);
    const auto pagesBefore = relation->relpages;
    const_cast<PgClassRow*>(relation)->relpages = pagesBefore + 73;
    assert(engine.rollbackToSavepoint("read_only_image") == DBStatus::OK);
    const auto* restoredRelation = engine.catalogService().get(db).findClass(relationId);
    assert(restoredRelation && restoredRelation->relpages == pagesBefore);
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);

    // A direct storage API schema rewrite, without a DDL epoch/setter, must
    // still force the original physical rollback and restore the old shape.
    const Column extra = makeIntColumn("extra", true, 0);
    assert(engine.alterTableAddColumn(db, "items", extra) == DBStatus::OK);
    assert(engine.getTableSchema(db, "items").len == 2);
    assert(engine.rollbackToSavepoint("read_only_image") == DBStatus::OK);
    assert(engine.getTableSchema(db, "items").len == 1);
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);

    // A physical write invisible to all transaction log-size/epoch counters
    // also invalidates the proof: it cannot survive ROLLBACK TO.
    const auto rawSidecar = engine.dbPath(db) / "direct-physical-sidecar";
    {
        std::ofstream out(rawSidecar, std::ios::binary);
        out << "changed outside the logical mutation tracking\n";
    }
    assert(engine.rollbackToSavepoint("read_only_image") == DBStatus::OK);
    assert(!fs::exists(rawSidecar));

    // Real row DML and repeated rollback retain the exact snapshot contents.
    assert(engine.insert(db, "items", {{"id", "8"}}) == DBStatus::OK);
    assert(engine.rollbackToSavepoint("read_only_image") == DBStatus::OK);
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);
    assert(engine.releaseSavepoint("read_only_image") == DBStatus::OK);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assert(!engine.tableExists(db, "items"));

    // A consumed/damaged image never licenses a skipped restore even when
    // the live heap itself has not changed.
    assert(engine.beginTransaction(db, true) == DBStatus::OK);
    assert(engine.createTransactionBackup());
    engine.preserveTransactionBackupOnRollback(true);
    engine.restoreTransactionBackupBeforeRowUndo(true);
    engine.markTransactionBackupDirty();
    assert(engine.createTable(db, table) == DBStatus::OK);
    assert(engine.savepoint("damaged_image") == DBStatus::OK);
    size_t damaged = 0;
    for (const auto& entry : fs::directory_iterator(fs::current_path())) {
        if (entry.path().filename().string().rfind(db + ".ddl_statement_backup.", 0) != 0)
            continue;
        for (const auto& file : fs::directory_iterator(entry.path())) {
            if (file.path().filename() == "items.stc") {
                std::ofstream out(file.path(), std::ios::binary | std::ios::app);
                out << "damaged snapshot bytes";
                ++damaged;
            }
        }
    }
    assert(damaged == 1);
    assert(engine.rollbackToSavepoint("damaged_image") == DBStatus::IO_ERROR);
    assert(!engine.inTransaction());
    assert(!engine.tableExists(db, "items"));
    std::cout << "savepoint unchanged image and conservative mutation fallback OK\n";
}
