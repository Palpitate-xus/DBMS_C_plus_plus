#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <malloc.h>

extern dbms::StorageEngine g_engine;

static size_t liveBytes() {
    const auto info = mallinfo2();
    return info.uordblks + info.hblkhd;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("table_scan_streaming");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "rows";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 2));
    schema.append(makeTextColumn("payload", true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    constexpr size_t count = 192, width = 16384;
    const auto payload = [](size_t id) {
        if (id % 23 == 0) return std::optional<std::string>{};
        if (id % 29 == 0) return std::optional<std::string>("");
        if (id % 31 == 0) return std::optional<std::string>("NULL");
        return std::optional<std::string>(std::string(width, 'a' + id % 23));
    };
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    for (size_t id = 0; id < count; ++id)
        assert(g_engine.insertRow(database, schema.tablename,
            {{"id", std::to_string(id)}, {"payload", payload(id)}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    schema = g_engine.getTableSchema(database, schema.tablename);
    // Warm the real heap/TOAST pools without keeping any row strings.
    assert(g_engine.forEachRow(database, schema.tablename,
        [&](uint32_t, uint16_t, const char* data, size_t length) {
            bool ok = false;
            (void)g_engine.resolveToastValues(database, schema.tablename,
                                             std::string(data, length), schema, &ok);
            assert(ok);
        }));
    StorageEngine::unbindNullRow();
    size_t controls = 0, failures = 0;
    const auto check = [&](const char* label, bool passed) {
        ++controls; failures += !passed;
        std::cout << "TABLE_SCAN " << label << " passed=" << passed << '\n';
    };
    TableScanOp scan(&g_engine, database, schema.tablename);
    const auto before = liveBytes();
    assert(scan.open());
    const auto after = liveBytes();
    const auto retained = after > before ? after - before : 0;
    std::cout << "TABLE_SCAN retained_at_open_bytes=" << retained << '\n';
    check("bounded open allocations", retained < 1024 * 1024);
    std::string row;
    assert(scan.next(row));
    check("first row identity", scan.lastRid() > 0 &&
          g_engine.extractColumnValue(row, schema, 0, database) == "0");
    check("physical NULL", scan.lastColumnIsNull(1));
    scan.close();
    check("early close locks", g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(scan.open());
    size_t seen = 0;
    bool exact = true;
    while (scan.next(row)) {
        const auto expected = payload(seen);
        bool isNull = false;
        const auto value = g_engine.extractColumnValue(row, schema, 1, database, true, &isNull);
        exact = exact && g_engine.extractColumnValue(row, schema, 0, database) == std::to_string(seen)
            && scan.lastColumnIsNull(1) == !expected && isNull == !expected
            && (expected ? value == *expected : value.empty());
        ++seen;
    }
    check("complete values after reopen", exact && seen == count && !scan.hasError());
    scan.close();
    check("complete close locks", g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "TABLE_SCAN_CHECKED=" << controls << " FAILURES=" << failures << '\n';
    return failures ? 1 : 0;
}
