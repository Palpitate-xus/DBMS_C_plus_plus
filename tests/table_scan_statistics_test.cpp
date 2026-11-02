#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "process/RuntimeStats.h"
#include "storage/PageAllocator.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("table_scan_statistics");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "rows";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", true, 2));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    for (const std::optional<std::string>& id : {std::optional<std::string>{}, {"1"}, {"2"}})
        assert(g_engine.insertRow(database, schema.tablename, {{"id", id}}) == DBStatus::OK);
    schema = g_engine.getTableSchema(database, schema.tablename);
    resetRuntimeTableStats(database, schema.tablename);
    const auto stats = [&] {
        const auto tables = getRuntimeTableStats(database);
        for (const auto& row : tables) if (row.relname == schema.tablename) return row;
        assert(false); return RuntimeTableStats{};
    };
    std::string row;
    {
        TableScanOp scan(&g_engine, database, schema.tablename);
        assert(scan.open() && scan.next(row));
        { TableScanOp unopened(&g_engine, database, schema.tablename); }
        bool null = false;
        assert(g_engine.extractColumnValue(row, schema, 0, database, true, &null).empty() && null);
        // Destruction of an early-stopped scan must close its cursor/lock.
    }
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    auto current = stats();
    assert(current.seqScan == 1 && current.seqTupRead == 1 && !current.liveTupEstimateValid);
    {
        TableScanOp scan(&g_engine, database, schema.tablename);
        assert(scan.open());
        size_t seen = 0;
        while (scan.next(row)) ++seen;
        assert(seen == 3 && !scan.hasError());
        scan.close();
    }
    current = stats();
    assert(current.seqScan == 2 && current.seqTupRead == 4 &&
           current.nLiveTup == 3 && current.liveTupEstimateValid);
    {
        TableScanOp scan(&g_engine, database, schema.tablename);
        assert(scan.open() && scan.next(row));
        scan.close(); scan.close();
    }
    current = stats();
    assert(current.seqScan == 3 && current.seqTupRead == 5 &&
           current.nLiveTup == 3 && current.liveTupEstimateValid);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    {
        TableScanOp scan(&g_engine, database, schema.tablename);
        assert(scan.open());
        // Lose the real allocator after open but before the first page is
        // requested. The resulting read error must release its page lock.
        g_engine.getPageAllocator(database, schema.tablename)->close();
        bool failed = false;
        try { (void)scan.next(row); }
        catch (const DbError& error) { failed = error.sqlState() == "58030"; }
        assert(failed);
        scan.close();
        const auto locks = g_engine.getLockManager().captureCheckpoint();
        assert(locks.tableCounts.empty() && locks.pageModes.empty());
        assert(scan.open());
        size_t seen = 0;
        while (scan.next(row)) ++seen;
        assert(seen == 3 && !scan.hasError());
        scan.close();
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[TABLE SCAN STATISTICS] partial/full/destructor/error ownership passed\n";
}
