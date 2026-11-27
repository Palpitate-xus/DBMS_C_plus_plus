#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/WAL.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

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
        assert(!injected_);
        std::ofstream output(path_, std::ios::binary | std::ios::app);
        assert(output);
        output.put(static_cast<char>(0x5a));
        output.flush();
        assert(output.good());
        injected_ = true;
    }

    void restore() {
        assert(injected_);
        std::filesystem::resize_file(path_, originalSize_);
        injected_ = false;
    }

    bool injected() const { return injected_; }

private:
    dbms::WALManager* wal_ = nullptr;
    std::filesystem::path path_;
    uintmax_t originalSize_ = 0;
    bool injected_ = false;
};

void assertOriginal(const std::string& database, const std::string& original,
                    const std::string& changed) {
    assert(g_engine.query(database, "t", {"=value " + original}, {"id"})
               .size() == 1);
    assert(g_engine.query(database, "t", {"=value " + changed}, {"id"})
               .empty());
    int64_t rid = -1;
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    assert(primary->search("1", rid));
}

void test_update_stops_before_heap_when_wal_fails(bool reclaimedPage) {
    const std::string testName = reclaimedPage ? "update_reclaimed_wal_failure" : "update_wal_failure";
    const std::string original = reclaimedPage ? std::string(1700, 'o') : "original";
    const std::string changed = reclaimedPage ? std::string(1700, 'c') : "changed";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, reclaimedPage ? 1700 : 40));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", original}}) ==
           dbms::DBStatus::OK);
    if (reclaimedPage) {
        const std::string filler(1700, 'z');
        for (int id = 2; id <= 4; ++id) {
            assert(g_engine.insert(database, "t", {{"id", std::to_string(id)}, {"value", filler}}) ==
                   dbms::DBStatus::OK);
        }
        assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, "t", {{"id", "5"}, {"value", filler}}) ==
               dbms::DBStatus::OK);
        assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
        assert(g_engine.vacuum(database, "t") == 1);
        auto* pages = g_engine.getPageAllocator(database, "t");
        assert(pages && pages->numPages() == 3 && !pages->isWritablePage(2));
    }
    assert(g_engine.createTrigger(database, {
               "break_wal", "before", "update", "t", "break_wal", "",
               true, true, {}}) == dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "repair_wal", "after", "update", "t", "repair_wal", "",
               false, true, {}}) == dbms::DBStatus::OK);

    WalTailFailure failure(database);
    bool beforeFired = false;
    bool afterFired = false;
    g_engine.setTriggerExecutor([&](const std::string& action) {
        if (action == "break_wal") {
            failure.inject();
            beforeFired = true;
        } else if (action == "repair_wal") {
            if (failure.injected()) failure.restore();
            afterFired = true;
        }
        return false;
    });

    const dbms::DBStatus status = g_engine.update(
        database, "t", {{"value", changed}}, {"=id 1"});
    g_engine.setTriggerExecutor({});
    if (failure.injected()) failure.restore();

    assert(beforeFired);
    assert(!afterFired);
    assert(status == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assertOriginal(database, original, changed);
    if (reclaimedPage) {
        auto* pages = g_engine.getPageAllocator(database, "t");
        assert(pages && pages->numPages() == 3 && pages->isWritablePage(2));
    }

    // A healthy WAL accepts the same UPDATE after the failed attempt.
    assert(g_engine.update(
               database, "t", {{"value", changed}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(database, "t", {"=value " + changed}, {"id"})
               .size() == 1);
    if (reclaimedPage) assert(g_engine.getPageAllocator(database, "t")->numPages() == 3);

    cleanupTestDb(testName);
    std::cout << "[UPDATE WAL FAILURE] failed WAL left heap and index untouched OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_update_stops_before_heap_when_wal_fails(false);
    test_update_stops_before_heap_when_wal_fails(true);
    finalCleanupTestData();
    return 0;
}
