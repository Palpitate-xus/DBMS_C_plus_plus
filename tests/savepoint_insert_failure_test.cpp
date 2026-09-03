#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/PgPage.h"
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

void test_failed_savepoint_undo_aborts_outer_transaction() {
    const std::string testName = "savepoint_insert_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.savepoint("keep_first") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    HeapPageFailure failure(database);
    failure.inject();
    const dbms::DBStatus status =
        g_engine.rollbackToSavepoint("keep_first");
    failure.restore();

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {}, {"id"}).empty());

    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    int64_t rid = -1;
    assert(!primary->search("0", rid));
    assert(!primary->search("1", rid));
    assert(g_engine.insert(database, "t", {{"id", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    std::cout << "[SAVEPOINT INSERT FAILURE] failed undo aborted outer transaction OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_failed_savepoint_undo_aborts_outer_transaction();
    finalCleanupTestData();
    return 0;
}
