// ============================================================================
// Partitioning execution test — Phase 4 Wave 4.26
// The partitioning machinery (CREATE TABLE ... PARTITION BY, PARTITION OF,
// INSERT routing, ALTER TABLE ATTACH/DETACH PARTITION, sub-partitioning, and
// full-table scan over all partitions) is already implemented in the engine.
// This test exercises it end-to-end through the engine API by building
// TableSchema directly, because the DDL AST bridge currently does not forward
// partition metadata to StorageEngine::createTable.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static dbms::TableSchema makeSchema(const std::string& tname, const std::vector<std::string>& colDefs) {
    dbms::TableSchema tbl;
    tbl.tablename = tname;
    for (const auto& def : colDefs) {
        size_t sp = def.find(' ');
        std::string cname = def.substr(0, sp);
        std::string ctype = (sp == std::string::npos) ? "" : def.substr(sp + 1);
        dbms::Column col;
        if (ctype == "int") col = dbms::makeIntColumn(cname, true, 2);
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

// Range-partitioned table: rows route to p1/p2/p3 based on year.
static void test_range_partitioning() {
    std::string db = testDbPath("part_range");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    auto tbl = makeSchema("logs", {"id int", "yr int"});
    tbl.partitionType = dbms::TableSchema::PartitionType::Range;
    tbl.partitionKey = "yr";
    tbl.rangePartitions = {{"p1", "2020"}, {"p2", "2025"}, {"p3", "MAXVALUE"}};
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    assert(g_engine.insert(db, "logs", {{"id", "1"}, {"yr", "2018"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "2"}, {"yr", "2022"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "3"}, {"yr", "2030"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "4"}, {"yr", "2099"}}) == dbms::DBStatus::OK);

    assert(rowCount(db, "logs") == 4);
    auto rows = readRowKeyedById(db, "logs");
    assert(rows["1"] == "2018");
    assert(rows["3"] == "2030");

    // ATTACH a new range partition and insert into it.
    auto res = g_engine.attachPartition(db, "logs", "p4", "FOR VALUES FROM (2100) TO (2200)");
    assert(res == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "logs", {{"id", "5"}, {"yr", "2150"}}) == dbms::DBStatus::OK);
    assert(rowCount(db, "logs") == 5);

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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_range_partitioning();
    test_list_partitioning();
    test_hash_partitioning();
    test_subpartitioning();
    std::cout << "[PART] all passed" << std::endl;
    return 0;
}
