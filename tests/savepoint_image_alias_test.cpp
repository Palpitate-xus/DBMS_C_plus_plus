#include "commands/TableManage.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;
using namespace dbms;

static size_t imageCount(const std::string& database) {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(fs::current_path()))
        if (entry.is_directory() &&
            entry.path().filename().string().rfind(database + ".ddl_statement_backup.", 0) == 0)
            ++count;
    return count;
}

static void beginPhysical(StorageEngine& engine, const std::string& database) {
    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    assert(engine.createTransactionBackup());
    engine.preserveTransactionBackupOnRollback(true);
    engine.restoreTransactionBackupBeforeRowUndo(true);
    engine.markTransactionBackupDirty();
}

int main() {
    const std::string database = "savepoint_shared_image_db";
    StorageEngine engine;
    assert(engine.createDatabase(database) == DBStatus::OK);
    beginPhysical(engine, database);
    TableSchema schema;
    schema.tablename = "items";
    schema.append(makeIntColumn("id", false, 2, true));
    schema.pkColIndices.push_back(0);
    assert(engine.createTable(database, schema) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(engine.savepoint("outer") == DBStatus::OK);
    assert(imageCount(database) == 1);
    assert(engine.savepoint("same") == DBStatus::OK);
    if (imageCount(database) != 1) {
        std::cerr << "unchanged nested savepoint duplicated the complete database image\n";
        assert(false);
    }
    assert(engine.savepoint("same") == DBStatus::OK);
    assert(imageCount(database) == 1);
    assert(engine.insert(database, "items", {{"id", "2"}}) == DBStatus::OK);
    assert(engine.rollbackToSavepoint("same") == DBStatus::OK);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);
    assert(engine.releaseSavepoint("same") == DBStatus::OK);
    // Releasing the newest name must not consume its older alias's image.
    assert(engine.rollbackToSavepoint("same") == DBStatus::OK);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);
    assert(engine.releaseSavepoint("same") == DBStatus::OK);
    assert(imageCount(database) == 1);
    assert(engine.insert(database, "items", {{"id", "3"}}) == DBStatus::OK);
    assert(engine.rollbackToSavepoint("outer") == DBStatus::OK);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);
    assert(imageCount(database) == 1);
    assert(engine.prepareTransaction("aliased_ddl_must_remain_local") == DBStatus::INVALID_VALUE);
    assert(engine.inTransaction());
    assert(imageCount(database) == 1);
    assert(engine.commitTransaction() == DBStatus::OK);
    assert(imageCount(database) == 0);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);

    beginPhysical(engine, database);
    assert(engine.alterTableAddColumn(database, "items", makeIntColumn("extra", true, 0)) == DBStatus::OK);
    assert(engine.savepoint("outer") == DBStatus::OK);
    assert(engine.savepoint("inner") == DBStatus::OK);
    assert(imageCount(database) == 1);
    {
        std::ofstream out(engine.dbPath(database) / "direct-physical-change");
        out << "not tracked by any mutation epoch";
    }
    assert(engine.rollbackToSavepoint("inner") == DBStatus::OK);
    assert(!fs::exists(engine.dbPath(database) / "direct-physical-change"));
    assert(engine.releaseSavepoint("inner") == DBStatus::OK);
    assert(imageCount(database) == 1);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assert(imageCount(database) == 0);
    assert(engine.getTableSchema(database, "items").len == 1);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);

    // Equal logical boundaries alone are not proof that the physical image
    // is reusable. Capture the raw change in a distinct newer image, then
    // preserve it at the newer boundary and remove it at the older one.
    beginPhysical(engine, database);
    assert(engine.savepoint("before_raw") == DBStatus::OK);
    const auto untracked = engine.dbPath(database) / "untracked-before-savepoint";
    {
        std::ofstream out(untracked);
        out << "new preimage with unchanged logical mutation counters";
    }
    assert(engine.savepoint("after_raw") == DBStatus::OK);
    assert(imageCount(database) == 2);
    assert(engine.rollbackToSavepoint("after_raw") == DBStatus::OK);
    assert(fs::exists(untracked));
    assert(engine.releaseSavepoint("after_raw") == DBStatus::OK);
    assert(imageCount(database) == 1);
    assert(engine.rollbackToSavepoint("before_raw") == DBStatus::OK);
    assert(!fs::exists(untracked));
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assert(imageCount(database) == 0);

    beginPhysical(engine, database);
    assert(engine.savepoint("outer") == DBStatus::OK);
    assert(engine.savepoint("inner") == DBStatus::OK);
    assert(imageCount(database) == 1);
    for (const auto& image : fs::directory_iterator(fs::current_path())) {
        if (image.path().filename().string().rfind(database + ".ddl_statement_backup.", 0) != 0)
            continue;
        std::ofstream out(image.path() / "items.stc", std::ios::binary | std::ios::app);
        out << "damaged alias payload";
    }
    assert(engine.rollbackToSavepoint("inner") == DBStatus::IO_ERROR);
    assert(!engine.inTransaction());
    assert(imageCount(database) == 0);
    assert(engine.getTableSchema(database, "items").len == 1);
    assert(engine.query(database, "items", {}, {"id"}).size() == 1);
    std::cout << "shared savepoint images retain aliases and clean up once\n";
}
