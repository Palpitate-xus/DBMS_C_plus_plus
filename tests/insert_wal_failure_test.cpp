#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
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

void test_insert_stops_before_heap_when_wal_fails() {
    const std::string testName = "insert_wal_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "break_wal", "before", "insert", "t", "break_wal", "",
               true, true, {}}) == dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "repair_wal", "after", "insert", "t", "repair_wal", "",
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

    const dbms::DBStatus status =
        g_engine.insert(database, "t", {{"id", "1"}});
    g_engine.setTriggerExecutor({});
    if (failure.injected()) failure.restore();

    assert(beforeFired);
    assert(!afterFired);
    assert(status == dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).empty());
    int64_t rid = -1;
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    assert(primary != nullptr);
    assert(!primary->search("1", rid));

    // Repairing the injected WAL tail makes a later normal INSERT usable.
    assert(g_engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);

    cleanupTestDb(testName);
    std::cout << "[INSERT WAL FAILURE] failed WAL left heap and index untouched OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_insert_stops_before_heap_when_wal_fails();
    finalCleanupTestData();
    return 0;
}
