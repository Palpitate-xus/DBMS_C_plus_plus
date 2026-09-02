// ============================================================================
// Partitioning execution test — Phase 4 Wave 4.26
// The partitioning machinery (CREATE TABLE ... PARTITION BY, PARTITION OF,
// INSERT routing, ALTER TABLE ATTACH/DETACH PARTITION, sub-partitioning, and
// full-table scan over all partitions) is already implemented in the engine.
// This test exercises the lower-level TableSchema API; DDL-level wiring is
// covered by create_table_options_test.cpp.
// ============================================================================

#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "storage/PageAllocator.h"
#include "storage/PageCrypto.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static dbms::TableSchema makeSchema(const std::string& tname, const std::vector<std::string>& colDefs) {
    dbms::TableSchema tbl;
    tbl.tablename = tname;
    for (const auto& def : colDefs) {
        size_t sp = def.find(' ');
        std::string cname = def.substr(0, sp);
        std::string ctype = (sp == std::string::npos) ? "" : def.substr(sp + 1);
        dbms::Column col;
        if (ctype == "int") col = dbms::makeIntColumn(cname, true, 2);
        else if (ctype == "date") col = dbms::makeDateColumn(cname, true);
        else if (ctype == "timestamp") col = dbms::makeTimestampColumn(cname, true);
        else if (ctype == "text") col = dbms::makeTextColumn(cname, true);
        else if (ctype == "varchar") col = dbms::makeVarCharColumn(cname, true, 20);
        else col = dbms::makeVarCharColumn(cname, true, 20);
        tbl.append(col);
    }
    return tbl;
}

static std::map<std::string, std::string> readRowKeyedById(const std::string& db, const std::string& tbl) {
    dbms::TableSchema schema = g_engine.getTableSchema(db, tbl);
    std::map<std::string, std::string> out;
    g_engine.forEachRow(db, tbl, [&](uint32_t, uint16_t, const char* data, size_t len) {
        std::string row(data, len);
        std::string id = g_engine.extractColumnValue(row, schema, 0);
        std::string v = g_engine.extractColumnValue(row, schema, 1);
        out[id] = v;
    });
    return out;
}

static size_t rowCount(const std::string& db, const std::string& tbl) {
    size_t n = 0;
    g_engine.forEachRow(db, tbl, [&](uint32_t, uint16_t, const char*, size_t) { ++n; });
    return n;
}

static size_t rowCountInPartitions(
    const std::string& db, const std::string& tbl,
    const std::vector<std::string>& partitions) {
    size_t n = 0;
    assert(g_engine.forEachRow(
        db, tbl,
        [&](uint32_t, uint16_t, const char*, size_t) { ++n; },
        nullptr, partitions));
    return n;
}

// Range-partitioned table: rows route to p1/p2/p3 based on year.
static void test_range_partitioning() {
    std::string db = testDbPath("part_range");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("logs", {"id int", "yr int"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "yr";
    tbl.rangePartitions = {{"p1", "2020"}, {"p2", "2025"}, {"p3", "MAXVALUE"}};
    tbl.cols[0].isPrimaryKey = true;
    tbl.pkColIndices = {0};
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    assert(g_engine.insert(db, "logs", {{"id", "1"}, {"yr", "2018"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "2"}, {"yr", "2022"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "3"}, {"yr", "2030"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "4"}, {"yr", "2099"}}) == dbms::DBStatus::OK);

    assert(rowCount(db, "logs") == 4);
    auto rows = readRowKeyedById(db, "logs");
    assert(rows["1"] == "2018");
    assert(rows["3"] == "2030");

    // Every physical partition starts at the same local page/slot pair.  The
    // current row-addressed UPDATE/DELETE implementation cannot retain that
    // identity, so it must reject the operation instead of reporting a
    // successful no-op or targeting an unrelated tuple.
    auto matches = g_engine.query(db, "logs", {"=id 2"}, {});
    assert(matches.size() == 1);
    dbms::PlanContext planContext;
    planContext.dbname = db;
    planContext.tablename = "logs";
    planContext.conds = {{"=", "id", "2"}};
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, planContext);
    const std::string explain =
        dbms::QueryPlanner::explain(plan, &g_engine, db);
    assert(explain.find("Index Scan") == std::string::npos);
    assert(plan->open());
    std::string plannedRow;
    assert(plan->next(plannedRow));
    assert(!plan->next(plannedRow));
    plan->close();
    assert(g_engine.update(db, "logs", {{"id", "20"}}, {"=id 2"}) ==
           dbms::DBStatus::INVALID_VALUE);
    rows = readRowKeyedById(db, "logs");
    assert(rows["2"] == "2022");
    assert(rows.count("20") == 0);
    assert(g_engine.remove(db, "logs", {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(rowCount(db, "logs") == 4);

    // Transaction undo has the same parent-only row locator limitation.
    // Reject before the sequence/WAL/heap state changes rather than accepting
    // an INSERT that ROLLBACK cannot remove.
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "6"}, {"yr", "2019"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(rowCount(db, "logs") == 4);

    // ATTACH a new range partition and insert into it.
    auto res = g_engine.attachPartition(db, "logs", "p4", "FOR VALUES FROM (2100) TO (2200)");
    assert(res == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "5"}, {"yr", "2150"}}) == dbms::DBStatus::OK);
    assert(rowCount(db, "logs") == 5);
    assert(rowCountInPartitions(db, "logs", {"p4"}) == 1);

    // DETACH removes the partition from routing.
    res = g_engine.detachPartition(db, "logs", "p4");
    assert(res == dbms::DBStatus::OK);
    assert(rowCount(db, "logs") == 4);

    cleanup(db);
    std::cout << "[PART] range partitioning OK" << std::endl;
}

// List-partitioned table with a DEFAULT partition.
static void test_list_partitioning() {
    std::string db = testDbPath("part_list");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("events", {"id int", "region varchar"});
    tbl.partitionType = dbms::TableSchema::PartitionType::List;
    tbl.partitionKey = "region";
    tbl.listPartitions = {{"east", {"NY", "NJ"}}, {"west", {"CA", "OR"}}};
    tbl.defaultPartitionName = "other";
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    assert(g_engine.insert(db, "events", {{"id", "1"}, {"region", "NY"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "events", {{"id", "2"}, {"region", "CA"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "events", {{"id", "3"}, {"region", "TX"}}) == dbms::DBStatus::OK);

    assert(rowCount(db, "events") == 3);
    auto rows = readRowKeyedById(db, "events");
    assert(rows["1"] == "NY");
    assert(rows["3"] == "TX");

    cleanup(db);
    std::cout << "[PART] list partitioning OK" << std::endl;
}

// RANGE DEFAULT is a physical leaf too: inserts routed past the final bound
// must remain visible to full scans and partition-pruned reads.
static void test_range_default_partition() {
    std::string db = testDbPath("part_range_default");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("measurements", {"id int", "bucket int"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "bucket";
    tbl.rangePartitions = {{"bounded", "20"}};
    tbl.defaultPartitionName = "fallback";
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);
    assert(std::filesystem::is_regular_file(
        std::filesystem::path(db) / "measurements#fallback.dt"));

    assert(g_engine.insert(
               db, "measurements", {{"id", "1"}, {"bucket", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "measurements", {{"id", "2"}, {"bucket", "30"}}) ==
           dbms::DBStatus::OK);
    assert(rowCount(db, "measurements") == 2);
    const auto fallbackRows =
        g_engine.query(db, "measurements", {"=bucket 30"}, {"id"});
    assert(fallbackRows.size() == 1);
    assert(fallbackRows.front().find('2') != std::string::npos);

    cleanup(db);
    std::cout << "[PART] range default partition OK" << std::endl;
}

static void test_typed_range_routing_and_predicates() {
    std::string db = testDbPath("part_range_typed");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("numbers", {"id int", "value int"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "value";
    tbl.rangePartitions = {{"lt10", "10"}, {"lt100", "100"}};
    tbl.defaultPartitionName = "overflow";
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    for (const auto& [id, value] :
         std::vector<std::pair<std::string, std::string>>{
             {"1", "2"}, {"2", "10"}, {"3", "20"}, {"4", "100"}}) {
        assert(g_engine.insert(
                   db, "numbers", {{"id", id}, {"value", value}}) ==
               dbms::DBStatus::OK);
    }
    assert(rowCountInPartitions(db, "numbers", {"lt10"}) == 1);
    assert(rowCountInPartitions(db, "numbers", {"lt100"}) == 2);
    assert(rowCountInPartitions(db, "numbers", {"overflow"}) == 1);

    assert(g_engine.query(db, "numbers", {"<value 10"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "numbers", {"<=value 10"}, {"id"}).size() == 2);
    assert(g_engine.query(db, "numbers", {">value 10"}, {"id"}).size() == 2);
    assert(g_engine.query(db, "numbers", {">=value 10"}, {"id"}).size() == 3);
    assert(g_engine.query(db, "numbers", {"=value 20"}, {"id"}).size() == 1);

    auto finite = makeSchema("finite", {"id int", "value int"});
    finite.partitionType = dbms::TableSchema::PartitionType::Range;
    finite.partitionKey = "value";
    finite.rangePartitions = {{"lt10", "10"}};
    assert(g_engine.createTable(db, finite) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "finite", {{"id", "1"}, {"value", "10"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    // Date ordering must not depend on zero padding or separator spelling.
    auto dates = makeSchema("dates", {"id int", "value date"});
    dates.partitionType = dbms::TableSchema::PartitionType::Range;
    dates.partitionKey = "value";
    dates.rangePartitions = {
        {"before_february", "2024/2/1"}, {"later", "2025-1-1"}};
    assert(g_engine.createTable(db, dates) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "dates", {{"id", "1"}, {"value", "2024-01-31"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "dates", {{"id", "2"}, {"value", "2024-10-01"}}) ==
           dbms::DBStatus::OK);
    assert(rowCountInPartitions(
               db, "dates", {"before_february"}) == 1);
    assert(rowCountInPartitions(db, "dates", {"later"}) == 1);

    // MAXVALUE is special only in catalog bounds.  The same text in a row is
    // an ordinary collatable value and sorts before the literal upper bound N.
    auto words = makeSchema("words", {"id int", "value text"});
    words.partitionType = dbms::TableSchema::PartitionType::Range;
    words.partitionKey = "value";
    words.rangePartitions = {{"before_n", "N"}, {"remainder", "MAXVALUE"}};
    assert(g_engine.createTable(db, words) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "words", {{"id", "1"}, {"value", "MAXVALUE"}}) ==
           dbms::DBStatus::OK);
    assert(rowCountInPartitions(db, "words", {"before_n"}) == 1);

    // Contiguous ATTACH operations used to compare the existing upper bound
    // to the new lower bound, reversing adjacent ranges with equal endpoints.
    auto attached = makeSchema("attached", {"id int", "value int"});
    attached.partitionType = dbms::TableSchema::PartitionType::Range;
    attached.partitionKey = "value";
    assert(g_engine.createTable(db, attached) == dbms::DBStatus::OK);
    assert(g_engine.attachPartition(
               db, "attached", "lt10", "FOR VALUES FROM (0) TO (10)") ==
           dbms::DBStatus::OK);
    assert(g_engine.attachPartition(
               db, "attached", "lt100", "FOR VALUES FROM (10) TO (100)") ==
           dbms::DBStatus::OK);
    const auto attachedSchema = g_engine.getTableSchema(db, "attached");
    assert(attachedSchema.rangePartitions.size() == 2);
    assert(attachedSchema.rangePartitions[0].first == "lt10");
    assert(attachedSchema.rangePartitions[1].first == "lt100");
    assert(g_engine.insert(
               db, "attached", {{"id", "1"}, {"value", "20"}}) ==
           dbms::DBStatus::OK);
    assert(rowCountInPartitions(db, "attached", {"lt100"}) == 1);

    cleanup(db);
    std::cout << "[PART] typed range routing and predicates OK" << std::endl;
}

// Hash-partitioned table: rows distribute across p0/p1/p2/p3.
static void test_hash_partitioning() {
    std::string db = testDbPath("part_hash");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("htable", {"id int", "key varchar"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Hash;
    tbl.partitionKey = "key";
    tbl.hashPartitions = 4;
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    for (int i = 1; i <= 20; ++i)
        assert(g_engine.insert(db, "htable", {{"id", std::to_string(i)}, {"key", "k" + std::to_string(i)}}) == dbms::DBStatus::OK);

    assert(rowCount(db, "htable") == 20);

    // ATTACH p4 (must be the next hash partition).
    auto res = g_engine.attachPartition(db, "htable", "p4", "");
    assert(res == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "htable", {{"id", "21"}, {"key", "k21"}}) == dbms::DBStatus::OK);
    assert(rowCount(db, "htable") == 21);

    cleanup(db);
    std::cout << "[PART] hash partitioning OK" << std::endl;
}

// Sub-partitioning: RANGE partition + HASH sub-partition on a second column.
static void test_subpartitioning() {
    std::string db = testDbPath("part_sub");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("subtbl", {"id int", "yr int", "tag varchar"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "yr";
    tbl.rangePartitions = {{"p2020", "2021"}, {"p2025", "2026"}};
    tbl.subPartitionType = dbms::TableSchema::PartitionType::Hash;
    tbl.subPartitionKey = "tag";
    tbl.subHashPartitions = 2;
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    assert(g_engine.insert(db, "subtbl", {{"id", "1"}, {"yr", "2020"}, {"tag", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "subtbl", {{"id", "2"}, {"yr", "2020"}, {"tag", "b"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "subtbl", {{"id", "3"}, {"yr", "2025"}, {"tag", "c"}}) == dbms::DBStatus::OK);

    assert(rowCount(db, "subtbl") == 3);

    cleanup(db);
    std::cout << "[PART] sub-partitioning OK" << std::endl;
}

static void test_drop_removes_partition_storage() {
    std::string db = testDbPath("part_drop_storage");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("dropme", {"id int", "bucket int", "tag text"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "bucket";
    tbl.rangePartitions = {{"low", "10"}, {"high", "20"}};
    tbl.defaultPartitionName = "overflow";
    tbl.subPartitionType = dbms::TableSchema::PartitionType::Hash;
    tbl.subPartitionKey = "tag";
    tbl.subHashPartitions = 2;
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    const std::vector<std::string> partitions = {"low", "high", "overflow"};
    std::vector<std::filesystem::path> relationFiles;
    for (const auto& partition : partitions) {
        for (size_t sub = 0; sub < tbl.subHashPartitions; ++sub) {
            const auto heap = std::filesystem::path(db) /
                ("dropme#" + partition + "#sp" + std::to_string(sub) +
                 ".dt");
            relationFiles.push_back(heap);
            relationFiles.emplace_back(heap.string() + ".tde");
        }
    }
    for (const auto& path : relationFiles) {
        assert(std::filesystem::is_regular_file(path));
    }
    assert(g_engine.insert(
               db, "dropme",
               {{"id", "1"}, {"bucket", "5"}, {"tag", "left"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "dropme",
               {{"id", "2"}, {"bucket", "25"}, {"tag", "right"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.dropTable(db, "dropme") == dbms::DBStatus::OK);
    for (const auto& path : relationFiles) {
        assert(!std::filesystem::exists(path));
    }

    // Reusing the relation name must start with empty partition heaps rather
    // than reopening tuples left behind by the dropped table.
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);
    assert(rowCount(db, "dropme") == 0);

    cleanup(db);
    std::cout << "[PART] DROP removes partition storage OK" << std::endl;
}

static std::string incompressiblePayload(size_t length) {
    std::string payload;
    payload.reserve(length);
    uint32_t state = 0x12345678u;
    for (size_t index = 0; index < length; ++index) {
        state = state * 1664525u + 1013904223u;
        payload.push_back(static_cast<char>(33 + state % 90));
    }
    return payload;
}

static void test_rename_preserves_partition_storage() {
    std::string db = testDbPath("part_rename_storage");
    cleanup(db);
    dbms::PageCrypto::disable();
    assert(dbms::PageCrypto::enable(std::string(64, '8')));
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema(
        "old_events", {"id int", "bucket int", "tag text", "payload text"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "bucket";
    tbl.rangePartitions = {{"low", "10"}, {"high", "20"}};
    tbl.defaultPartitionName = "overflow";
    tbl.subPartitionType = dbms::TableSchema::PartitionType::Hash;
    tbl.subPartitionKey = "tag";
    tbl.subHashPartitions = 2;
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    const std::string payload = incompressiblePayload(9000);
    assert(g_engine.insert(
               db, "old_events",
               {{"id", "1"}, {"bucket", "5"}, {"tag", "left"},
                {"payload", payload}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "old_events",
               {{"id", "2"}, {"bucket", "25"}, {"tag", "right"},
                {"payload", "inline"}}) == dbms::DBStatus::OK);
    assert(g_engine.checkpoint(db));

    const std::vector<std::string> partitions = {"low", "high", "overflow"};
    std::vector<std::filesystem::path> oldFiles;
    std::vector<std::filesystem::path> newFiles;
    for (const auto& partition : partitions) {
        for (size_t sub = 0; sub < tbl.subHashPartitions; ++sub) {
            const std::string suffix = "#" + partition + "#sp" +
                std::to_string(sub) + ".dt";
            const auto oldHeap =
                std::filesystem::path(db) / ("old_events" + suffix);
            const auto newHeap =
                std::filesystem::path(db) / ("new_events" + suffix);
            oldFiles.push_back(oldHeap);
            oldFiles.emplace_back(oldHeap.string() + ".tde");
            newFiles.push_back(newHeap);
            newFiles.emplace_back(newHeap.string() + ".tde");
        }
    }
    for (const char* suffix : {
             ".toastmeta", ".toast.dt", ".toast.dt.tde",
             ".toast.idx", ".toast.idx.tde"}) {
        oldFiles.emplace_back(
            std::filesystem::path(db) /
            (std::string("old_events") + suffix));
        newFiles.emplace_back(
            std::filesystem::path(db) /
            (std::string("new_events") + suffix));
    }
    for (const auto& path : oldFiles) {
        assert(std::filesystem::is_regular_file(path));
    }

    assert(g_engine.alterTableRenameTable(
               db, "old_events", "new_events") == dbms::DBStatus::OK);
    assert(!g_engine.tableExists(db, "old_events"));
    assert(g_engine.tableExists(db, "new_events"));
    for (const auto& path : oldFiles) assert(!std::filesystem::exists(path));
    for (const auto& path : newFiles) {
        assert(std::filesystem::is_regular_file(path));
    }

    auto rows = g_engine.query(db, "new_events", {"=id 1"}, {"payload"});
    assert(rows.size() == 1);
    assert(rows.front().find(payload) != std::string::npos);
    {
        dbms::StorageEngine reopened;
        rows = reopened.query(
            db, "new_events", {"=id 2"}, {"payload"});
        assert(rows.size() == 1);
        assert(rows.front().find("inline") != std::string::npos);
    }

    assert(g_engine.dropTable(db, "new_events") == dbms::DBStatus::OK);
    cleanup(db);
    dbms::PageCrypto::disable();
    std::cout << "[PART] RENAME preserves partition storage OK" << std::endl;
}

// Partition page images must replay into the physical partition fork.  The
// parent heap is intentionally empty and must stay empty across startup.
static void test_partition_wal_routing() {
    std::string db = testDbPath("part_wal");
    cleanup(db);
    {
        dbms::StorageEngine writer;
        assert(writer.createDatabase(db, "utf8") == dbms::DBStatus::OK);

        auto tbl = makeSchema("walpart", {"id int", "yr int"});
        tbl.partitionType = dbms::TableSchema::PartitionType::Range;
        tbl.partitionKey = "yr";
        tbl.rangePartitions = {{"old", "2020"}, {"new", "MAXVALUE"}};
        assert(writer.createTable(db, tbl) == dbms::DBStatus::OK);
        assert(writer.insert(db, "walpart", {{"id", "1"}, {"yr", "2019"}}) ==
               dbms::DBStatus::OK);
        assert(writer.insert(db, "walpart", {{"id", "2"}, {"yr", "2024"}}) ==
               dbms::DBStatus::OK);

        auto* parent = writer.getPageAllocator(db, "walpart");
        assert(parent && parent->numPages() == 1);
    }

    // Model loss of both partition heaps after WAL reached disk.  Recovery
    // must reconstruct each file from its own fork images; merely preserving
    // the already-flushed files would not exercise redo routing.
    assert(std::filesystem::remove(
        std::filesystem::path(db) / "walpart#old.dt"));
    assert(std::filesystem::remove(
        std::filesystem::path(db) / "walpart#new.dt"));

    {
        dbms::StorageEngine reopened;
        auto* reopenedParent = reopened.getPageAllocator(db, "walpart");
        assert(reopenedParent && reopenedParent->numPages() == 1);
        size_t count = 0;
        assert(reopened.forEachRow(
            db, "walpart",
            [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
        assert(count == 2);
    }

    cleanup(db);
    std::cout << "[PART] WAL partition routing OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_range_partitioning();
    test_list_partitioning();
    test_range_default_partition();
    test_typed_range_routing_and_predicates();
    test_hash_partitioning();
    test_subpartitioning();
    test_drop_removes_partition_storage();
    test_rename_preserves_partition_storage();
    test_partition_wal_routing();
    std::cout << "[PART] all passed" << std::endl;
    return 0;
}
