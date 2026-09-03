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

void test_insert_undo_reports_heap_failure_and_cleans_index() {
    const std::string testName = "rollback_insert_page_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    HeapPageFailure failure(database);
    failure.inject();
    const dbms::DBStatus rollbackStatus = g_engine.rollbackTransaction();
    failure.restore();

    assert(rollbackStatus == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).empty());
    int64_t rid = -1;
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    assert(!primary->search("1", rid));

    // The stale key from the failed heap cleanup must not make the aborted
    // value permanently unusable once storage is repaired.
    assert(g_engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);

    cleanupTestDb(testName);
    std::cout << "[ROLLBACK INSERT FAILURE] error reported and index cleaned OK"
              << std::endl;
}

void test_insert_undo_stops_before_heap_on_wal_failure() {
    const std::string testName = "rollback_insert_wal_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    WalTailFailure failure(database);
    failure.inject();
    const dbms::DBStatus rollbackStatus = g_engine.rollbackTransaction();
    failure.restore();

    assert(rollbackStatus == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).empty());
    int64_t rid = -1;
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    assert(!primary->search("2", rid));
    assert(g_engine.insert(database, "t", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    std::cout << "[ROLLBACK INSERT FAILURE] WAL failure left heap unpublished OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_insert_undo_reports_heap_failure_and_cleans_index();
    test_insert_undo_stops_before_heap_on_wal_failure();
    finalCleanupTestData();
    return 0;
}
