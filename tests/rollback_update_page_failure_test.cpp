#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/PgPage.h"
#include "storage/WAL.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

class HeapPageFailure {
public:
    explicit HeapPageFailure(const std::string& database)
        : path_(std::filesystem::path(database) / "t.dt"),
          original_(dbms::PgPage::PAGE_SIZE) {
        allocator_ = g_engine.getPageAllocator(database, "t");
        assert(allocator_ != nullptr);
        assert(allocator_->flush());
        std::ifstream input(path_, std::ios::binary);
        assert(input);
        input.seekg(static_cast<std::streamoff>(dbms::PgPage::PAGE_SIZE));
        input.read(original_.data(),
                   static_cast<std::streamsize>(original_.size()));
        assert(input.gcount() ==
               static_cast<std::streamsize>(original_.size()));
    }

    void inject() {
        allocator_->bufferPool()->invalidatePage(1);
        std::fstream file(path_, std::ios::in | std::ios::out |
                                    std::ios::binary);
        assert(file);
        constexpr size_t changedOffset = 128;
        file.seekp(static_cast<std::streamoff>(dbms::PgPage::PAGE_SIZE +
                                               changedOffset));
        file.put(static_cast<char>(original_[changedOffset] ^ 0x5a));
        file.flush();
        assert(file.good());
        allocator_->bufferPool()->invalidatePage(1);
    }

    void restore() {
        allocator_->bufferPool()->invalidatePage(1);
        std::fstream file(path_, std::ios::in | std::ios::out |
                                    std::ios::binary);
        assert(file);
        file.seekp(static_cast<std::streamoff>(dbms::PgPage::PAGE_SIZE));
        file.write(original_.data(),
                   static_cast<std::streamsize>(original_.size()));
        file.flush();
        assert(file.good());
        allocator_->bufferPool()->invalidatePage(1);
    }

private:
    dbms::PageAllocator* allocator_ = nullptr;
    std::filesystem::path path_;
    std::vector<char> original_;
};

class WalTailFailure {
public:
    explicit WalTailFailure(const std::string& database) {
        wal_ = g_engine.getWAL(database);
        assert(wal_ != nullptr);
        const dbms::Lsn end = wal_->currentWriteLsn();
        assert(end > 0);
        const uint32_t segment = static_cast<uint32_t>(
            end / dbms::WALManager::kSegmentSize);
        path_ = wal_->segmentPath(segment);
        originalSize_ = std::filesystem::file_size(path_);
    }

    void inject() {
        std::ofstream output(path_, std::ios::binary | std::ios::app);
        assert(output);
        output.put(static_cast<char>(0x5a));
        output.flush();
        assert(output.good());
    }

    void restore() {
        std::filesystem::resize_file(path_, originalSize_);
    }

private:
    dbms::WALManager* wal_ = nullptr;
    std::filesystem::path path_;
    uintmax_t originalSize_ = 0;
};

std::string createTable(const std::string& testName,
                        const std::string& oldValue = "old") {
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, 1600));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", oldValue}}) ==
           dbms::DBStatus::OK);
    return database;
}

void assertOldVersionAndIndexes(const std::string& database,
                                const std::string& oldValue = "old") {
    const auto oldRows =
        g_engine.query(database, "t", {"=id 1"}, {"value"});
    assert(oldRows.size() == 1);
    assert(oldRows.front().find(oldValue) != std::string::npos);
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    int64_t rid = -1;
    const bool hasOld = primary->search("1", rid);
    const bool hasNew = primary->search("2", rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"value"}).empty());
    assert(hasOld);
    assert(!hasNew);
}

void applyUpdate(const std::string& database,
                 const std::string& newValue = "new") {
    assert(g_engine.update(
               database, "t", {{"id", "2"}, {"value", newValue}},
               {"=id 1"}) == dbms::DBStatus::OK);
}

void test_full_rollback_repairs_indexes_when_update_page_is_bad() {
    const std::string testName = "rollback_update_page_failure";
    const std::string database = createTable(testName);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    applyUpdate(database);

    HeapPageFailure failure(database);
    failure.inject();
    const dbms::DBStatus status = g_engine.rollbackTransaction();
    failure.restore();

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::IO_ERROR);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());
    assertOldVersionAndIndexes(database);
    cleanupTestDb(testName);
}

void test_savepoint_rollback_aborts_and_repairs_update_indexes() {
    const std::string testName = "savepoint_update_page_failure";
    const std::string database = createTable(testName);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_update") == dbms::DBStatus::OK);
    applyUpdate(database);

    HeapPageFailure failure(database);
    failure.inject();
    const dbms::DBStatus status =
        g_engine.rollbackToSavepoint("before_update");
    failure.restore();

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::IO_ERROR);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());
    assertOldVersionAndIndexes(database);
    cleanupTestDb(testName);
}

void test_full_rollback_stages_update_until_wal_is_durable() {
    const std::string testName = "rollback_update_wal_failure";
    const std::string database = createTable(testName);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    applyUpdate(database);

    WalTailFailure failure(database);
    failure.inject();
    const dbms::DBStatus status = g_engine.rollbackTransaction();
    failure.restore();

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::IO_ERROR);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());
    assertOldVersionAndIndexes(database);
    cleanupTestDb(testName);
}

void test_savepoint_restores_update_versions(bool differentPages) {
    const std::string testName = differentPages
        ? "savepoint_update_distinct_pages" : "savepoint_update_same_page";
    // Five inline values fill most of a page and force a new version to a
    // second page. A single value leaves space for both versions.
    const std::string oldValue(1500, 'o');
    const std::string newValue(1500, 'n');
    const std::string database = createTable(testName, oldValue);
    if (differentPages) {
        for (int id = 3; id <= 6; ++id) {
            assert(g_engine.insert(database, "t",
                {{"id", std::to_string(id)}, {"value", oldValue}}) ==
                dbms::DBStatus::OK);
        }
    }
    auto* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    int64_t oldRid = -1;
    assert(primary->search("1", oldRid));
    uint32_t oldPage = 0;
    uint16_t oldSlot = 0;
    dbms::StorageEngine::decodeRid(oldRid, oldPage, oldSlot);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_update") == dbms::DBStatus::OK);
    for (int attempt = 0; attempt < 2; ++attempt) {
        applyUpdate(database, newValue);
        int64_t newRid = -1;
        assert(primary->search("2", newRid));
        uint32_t newPage = 0;
        uint16_t newSlot = 0;
        dbms::StorageEngine::decodeRid(newRid, newPage, newSlot);
        assert(newRid != oldRid);
        assert((newPage != oldPage) == differentPages);

        assert(g_engine.rollbackToSavepoint("before_update") ==
               dbms::DBStatus::OK);
        assertOldVersionAndIndexes(database, oldValue);
        auto* allocator = g_engine.getPageAllocator(database, "t");
        assert(allocator != nullptr);
        char* buffer = allocator->fetchPage(newPage);
        assert(buffer != nullptr);
        const char* tuple = nullptr;
        size_t length = 0;
        assert(!dbms::PgPage(buffer).get(
            static_cast<dbms::OffsetNumber>(newSlot + 1), tuple, length));
        allocator->unpinPage(newPage);
    }
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertOldVersionAndIndexes(database, oldValue);
    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_full_rollback_repairs_indexes_when_update_page_is_bad();
    test_savepoint_rollback_aborts_and_repairs_update_indexes();
    test_full_rollback_stages_update_until_wal_is_durable();
    test_savepoint_restores_update_versions(false);
    test_savepoint_restores_update_versions(true);
    finalCleanupTestData();
    std::cout << "[ROLLBACK UPDATE FAILURE] heap errors repair indexes OK"
              << std::endl;
    return 0;
}
