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

std::string createTable(const std::string& testName) {
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, 32));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", "old"}}) ==
           dbms::DBStatus::OK);
    return database;
}

void assertOldVersionAndIndexes(const std::string& database) {
    const auto oldRows =
        g_engine.query(database, "t", {"=id 1"}, {"value"});
    assert(oldRows.size() == 1);
    assert(oldRows.front().find("old") != std::string::npos);
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    int64_t rid = -1;
    const bool hasOld = primary->search("1", rid);
    const bool hasNew = primary->search("2", rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"value"}).empty());
    assert(hasOld);
    assert(!hasNew);
}

void applyUpdate(const std::string& database) {
    assert(g_engine.update(
               database, "t", {{"id", "2"}, {"value", "new"}},
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
    assert(!g_engine.inTransaction());
    assertOldVersionAndIndexes(database);
    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_full_rollback_repairs_indexes_when_update_page_is_bad();
    test_savepoint_rollback_aborts_and_repairs_update_indexes();
    test_full_rollback_stages_update_until_wal_is_durable();
    finalCleanupTestData();
    std::cout << "[ROLLBACK UPDATE FAILURE] heap errors repair indexes OK"
              << std::endl;
    return 0;
}
