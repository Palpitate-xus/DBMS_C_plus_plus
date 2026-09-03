#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/PgPage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
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
        assert(!injected_);
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
        injected_ = true;
    }

    void restore() {
        assert(injected_);
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
        injected_ = false;
    }

private:
    dbms::PageAllocator* allocator_ = nullptr;
    std::filesystem::path path_;
    std::vector<char> original_;
    bool injected_ = false;
};

void createTableWithRows(const std::string& database, int rowCount) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, 40));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    for (int id = 1; id <= rowCount; ++id) {
        assert(g_engine.insert(
                   database, "t",
                   {{"id", std::to_string(id)},
                    {"value", "row-" + std::to_string(id)}}) ==
               dbms::DBStatus::OK);
    }
}

void test_matcher_read_failure_aborts_delete() {
    const std::string database = testDbPath("delete_matcher_read_failure");
    cleanupTestDb("delete_matcher_read_failure");
    createTableWithRows(database, 2);

    HeapPageFailure failure(database);
    bool injected = false;
    const auto matcher = [&](const std::map<std::string, std::string>&) {
        if (!injected) {
            failure.inject();
            injected = true;
        }
        return true;
    };
    const dbms::DBStatus status =
        g_engine.remove(database, "t", {}, nullptr, matcher);
    failure.restore();

    assert(injected);
    assert(status == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {}, {"id"}).size() == 2);

    cleanupTestDb("delete_matcher_read_failure");
    std::cout << "[DELETE PAGE FAILURE] matcher read failure aborted OK"
              << std::endl;
}

void test_heap_fetch_failure_preserves_row_and_index() {
    const std::string database = testDbPath("delete_heap_fetch_failure");
    cleanupTestDb("delete_heap_fetch_failure");
    createTableWithRows(database, 1);

    HeapPageFailure failure(database);
    assert(g_engine.createTrigger(database, {
               "corrupt_heap", "before", "delete", "t", "select 1", "",
               false, true, {}}) == dbms::DBStatus::OK);
    bool fired = false;
    g_engine.setTriggerExecutor([&](const std::string&) {
        assert(!fired);
        failure.inject();
        fired = true;
        return true;
    });
    const dbms::DBStatus status =
        g_engine.remove(database, "t", {"=id 1"});
    g_engine.setTriggerExecutor({});
    failure.restore();

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(fired);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", "duplicate"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    cleanupTestDb("delete_heap_fetch_failure");
    std::cout << "[DELETE PAGE FAILURE] heap/index consistency preserved OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_matcher_read_failure_aborts_delete();
    test_heap_fetch_failure_preserves_row_and_index();
    finalCleanupTestData();
    return 0;
}
