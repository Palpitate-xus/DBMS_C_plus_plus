#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    // Construct all backends before transactions; constructors perform recovery.
    StorageEngine writer;
    const auto database = testDbPath("table_scan_snapshot");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    const std::vector<std::optional<IsolationLevel>> levels = {
        {}, IsolationLevel::READ_COMMITTED, IsolationLevel::REPEATABLE_READ,
        IsolationLevel::SERIALIZABLE};
    constexpr size_t count = 192;
    const auto payload = [](size_t id) { return std::string(8192, 'a' + id % 23); };
    size_t controls = 0, failures = 0;
    for (size_t mode = 0; mode < levels.size(); ++mode) {
        TableSchema schema;
        schema.tablename = "rows" + std::to_string(mode);
        schema.formatVersion = DATA_FILE_FORMAT_VERSION;
        schema.append(makeIntColumn("id", false, 2));
        schema.append(makeTextColumn("payload", true));
        assert(g_engine.createTable(database, schema) == DBStatus::OK);
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        for (size_t id = 0; id < count; ++id)
            assert(g_engine.insert(database, schema.tablename,
                {{"id", std::to_string(id)}, {"payload", payload(id)}}) == DBStatus::OK);
        assert(g_engine.commitTransaction() == DBStatus::OK);
        schema = g_engine.getTableSchema(database, schema.tablename);
        if (levels[mode]) {
            assert(g_engine.setIsolationLevel(*levels[mode]));
            assert(g_engine.beginTransaction(database) == DBStatus::OK);
        }
        TableScanOp scan(&g_engine, database, schema.tablename);
        assert(scan.open());
        // Commit changes to unread pages after the scan snapshot exists.
        // TOAST OLD versions must remain readable through that snapshot.
        bool writerOK = false;
        std::thread change([&] {
            writer.getLockManager().setLockTimeout(3000);
            writerOK = writer.beginTransaction(database) == DBStatus::OK &&
                writer.update(database, schema.tablename, {{"payload", "changed"}}, {"=id 180"}) == DBStatus::OK &&
                writer.remove(database, schema.tablename, {"=id 181"}) == DBStatus::OK &&
                writer.insert(database, schema.tablename, {{"id", "192"}, {"payload", "new"}}) == DBStatus::OK &&
                writer.commitTransaction() == DBStatus::OK;
            if (writer.inTransaction()) (void)writer.rollbackTransaction();
        });
        change.join();
        bool exact = writerOK;
        size_t seen = 0;
        std::string row;
        while (scan.next(row)) {
            exact = exact && seen < count &&
                g_engine.extractColumnValue(row, schema, 0, database) == std::to_string(seen) &&
                g_engine.extractColumnValue(row, schema, 1, database) == payload(seen);
            ++seen;
        }
        exact = exact && seen == count && !scan.hasError();
        scan.close();
        if (levels[mode]) exact = g_engine.commitTransaction() == DBStatus::OK && exact;
        ++controls; failures += !exact;
        std::cout << "SCAN_SNAPSHOT mode=" << mode << " rows=" << seen
                  << " passed=" << exact << '\n';
        assert(g_engine.setIsolationLevel(IsolationLevel::READ_COMMITTED));
        TableScanOp fresh(&g_engine, database, schema.tablename);
        assert(fresh.open());
        bool updated = false, inserted = false, deleted = true;
        seen = 0;
        while (fresh.next(row)) {
            const auto id = g_engine.extractColumnValue(row, schema, 0, database);
            const auto value = g_engine.extractColumnValue(row, schema, 1, database);
            if (id == "180") updated = value == "changed";
            if (id == "181") deleted = false;
            if (id == "192") inserted = value == "new";
            ++seen;
        }
        const bool current = updated && inserted && deleted && seen == count && !fresh.hasError();
        fresh.close();
        ++controls; failures += !current;
        std::cout << "SCAN_FRESH mode=" << mode << " rows=" << seen
                  << " passed=" << current << '\n';
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "SCAN_SNAPSHOT_CHECKED=" << controls << " FAILURES=" << failures << '\n';
    return failures ? 1 : 0;
}
