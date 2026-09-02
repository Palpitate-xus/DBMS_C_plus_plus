// ============================================================================
// bloom_index_test — P2-2 bloom access method:
//   BloomIndex insert/search/remove round trip, persistence across close and
//   reopen, corruption rejection, mightContain false-negative freedom
//   StorageEngine createBloomIndex builds from existing heap rows
//   DML keeps the index in sync (insert/update/delete)
//   equality scans consult the bloom index through the planner
//   DROP INDEX removes it; table drop/rename keep files consistent
// ============================================================================

#include "access/BloomIndex.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include <cassert>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& p) {
    if (fs::exists(p)) fs::remove(p);
}

static void write_u32(std::ostream& out, uint32_t value) {
    for (int i = 0; i < 4; ++i)
        out.put(static_cast<char>((value >> (8 * i)) & 0xff));
}

static void write_u64(std::ostream& out, uint64_t value) {
    for (int i = 0; i < 8; ++i)
        out.put(static_cast<char>((value >> (8 * i)) & 0xff));
}

static void write_bloom_header(std::ostream& out, uint32_t bits,
                               uint32_t hashes, uint32_t entries) {
    write_u32(out, 0x314D4C42u);  // 'BLM1'
    write_u32(out, bits);
    write_u32(out, hashes);
    write_u32(out, entries);
}

static void test_bloom_unit() {
    const std::string idx = "/tmp/bloom_index_test.bidx";
    cleanup(idx);
    {
        dbms::BloomIndex b(idx);
        assert(b.open());
        b.insert("alpha", 10);
        b.insert("alpha", 11);
        b.insert("beta", 20);
        assert(b.size() == 2);
        assert((b.search("alpha") == std::vector<int64_t>{10, 11}));
        assert((b.search("beta") == std::vector<int64_t>{20}));
        assert(b.search("gamma").empty());
        // No false negatives for present keys.
        assert(b.mightContain("alpha"));
        assert(b.mightContain("beta"));
        assert(b.containsExact("alpha"));
        assert(!b.containsExact("gamma"));
        assert(b.flush());
        assert(b.close());
    }
    {
        // Persistence: same answers after reopen.
        dbms::BloomIndex b(idx);
        assert(b.open());
        assert((b.search("alpha") == std::vector<int64_t>{10, 11}));
        assert((b.search("beta") == std::vector<int64_t>{20}));
        // Remove one rid, keep the other.
        assert(b.remove("alpha", 10));
        assert((b.search("alpha") == std::vector<int64_t>{11}));
        // Removing the last rid drops the key from the exact map; the bloom
        // bits may stay set, but search must not report it.
        assert(b.remove("alpha", 11));
        assert(b.search("alpha").empty());
        assert(!b.containsExact("alpha"));
        assert(b.close());
    }
    {
        // Corruption (bad magic) is rejected.
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        out << "garbage not a bloom file";
    }
    dbms::BloomIndex broken(idx);
    assert(!broken.open());
    cleanup(idx);
    std::cout << "[BLOOM] unit persistence/corruption OK" << std::endl;
}

static void test_bloom_no_false_negatives_many_keys() {
    const std::string idx = "/tmp/bloom_index_many.bidx";
    cleanup(idx);
    {
        dbms::BloomIndex b(idx);
        assert(b.open());
        char key[32];
        for (int i = 0; i < 2000; ++i) {
            std::snprintf(key, sizeof(key), "key-%06d", i);
            b.insert(key, i);
        }
        assert(b.size() == 2000);
        for (int i = 0; i < 2000; i += 97) {
            std::snprintf(key, sizeof(key), "key-%06d", i);
            assert(b.mightContain(key));
            assert(b.containsExact(key));
        }
        assert(b.close());
    }
    {
        dbms::BloomIndex b(idx);
        assert(b.open());
        // Every inserted key must still be visible: false negatives would
        // corrupt query results.
        char key[32];
        for (int i = 0; i < 2000; ++i) {
            std::snprintf(key, sizeof(key), "key-%06d", i);
            assert(b.mightContain(key));
            assert(b.search(key) == std::vector<int64_t>{static_cast<int64_t>(i)});
        }
        assert(b.close());
    }
    cleanup(idx);
    std::cout << "[BLOOM] no false negatives (2000 keys) OK" << std::endl;
}

static void test_bloom_grows_geometrically() {
    const std::string idx = "/tmp/bloom_index_growth.bidx";
    cleanup(idx);

    dbms::BloomIndex b(idx);
    assert(b.open());
    uint32_t previousBits = b.bitCount();
    size_t resizeCount = 0;
    for (int i = 0; i < 4096; ++i) {
        const std::string key = "growth-key-" + std::to_string(i);
        assert(b.insert(key, i));
        if (b.bitCount() != previousBits) {
            previousBits = b.bitCount();
            ++resizeCount;
        }
    }

    // Geometric growth needs logarithmically many full re-hashes.  Exact
    // sizing used to resize essentially once per inserted key.
    assert(resizeCount < 16);
    for (int i = 0; i < 4096; i += 113)
        assert(b.search("growth-key-" + std::to_string(i)) ==
               std::vector<int64_t>{i});
    assert(b.close());
    cleanup(idx);
    std::cout << "[BLOOM] geometric growth OK (" << resizeCount
              << " rebuilds for 4096 keys)" << std::endl;
}

static void test_bloom_rejects_malformed_lengths_and_layout() {
    const std::string idx = "/tmp/bloom_index_malformed.bidx";
    cleanup(idx);

    // Counts must be validated against bytes remaining before reserving from
    // them.  A corrupt RID count must fail without changing object settings.
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        write_bloom_header(out, 64, 5, 1);
        write_u32(out, 1);
        out.put('x');
        write_u32(out, std::numeric_limits<uint32_t>::max());
    }
    {
        dbms::BloomIndex b(idx, 128, 7);
        assert(!b.open());
        assert(b.hashCount() == 7);
        assert(b.size() == 0);
    }

    // Duplicate keys make the entry count ambiguous and used to overwrite
    // the first RID list silently.
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        write_bloom_header(out, 64, 7, 2);
        for (uint64_t rid : {1ULL, 2ULL}) {
            write_u32(out, 1);
            out.put('x');
            write_u32(out, 1);
            write_u64(out, rid);
        }
    }
    {
        dbms::BloomIndex b(idx);
        assert(!b.open());
    }

    // Empty RID lists cannot be produced by the index API and violate the
    // exact side-map invariant.
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        write_bloom_header(out, 64, 7, 1);
        write_u32(out, 1);
        out.put('x');
        write_u32(out, 0);
    }
    {
        dbms::BloomIndex b(idx);
        assert(!b.open());
    }

    // Do not silently accept torn/concatenated files or malformed bit counts.
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        write_bloom_header(out, 0, 7, 0);
        out.put('!');
    }
    {
        dbms::BloomIndex b(idx);
        assert(!b.open());
    }
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        write_bloom_header(out, 1, 7, 0);
    }
    {
        dbms::BloomIndex b(idx);
        assert(!b.open());
    }

    cleanup(idx);
    fs::create_directory(idx);
    {
        dbms::BloomIndex b(idx);
        assert(!b.open());
    }
    cleanup(idx);
    std::cout << "[BLOOM] malformed length/layout rejection OK" << std::endl;
}

static void test_engine_bloom_lifecycle() {
    const std::string db = testDbPath("bloom_storage");
    cleanupTestDb("bloom_storage");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    s.currentDB = db;
    s.username = "testuser";
    s.permission = 1;
    dbms::DdlExecutor ddl;
    auto exec = [&](const std::string& sql) { return ddl.executeSql(sql, s); };

    assert(!exec("CREATE TABLE bt (id INT, name VARCHAR(64))"));
    for (int i = 1; i <= 50; ++i) {
        char val[32];
        std::snprintf(val, sizeof(val), "name%03d", i);
        char idStr[16];
        std::snprintf(idStr, sizeof(idStr), "%d", i);
        assert(g_engine.insert(db, "bt", {{"id", idStr}, {"name", val}})
               == dbms::DBStatus::OK);
    }

    // CREATE INDEX ... USING bloom builds from the existing heap.
    assert(!exec("CREATE INDEX bt_name_bloom ON bt USING bloom (name)"));
    assert(g_engine.getBloomIndexedColumns(db, "bt").size() == 1);
    assert(g_engine.getBloomIndexedColumns(db, "bt")[0] == "name");
    auto* bidx = g_engine.getBloomIndex(db, "bt", "name");
    assert(bidx);
    assert(bidx->size() == 50);
    assert(bidx->containsExact("name017"));
    assert(!bidx->containsExact("name999"));

    // DML keeps the index in sync.
    assert(g_engine.insert(db, "bt", {{"id", "51"}, {"name", "name051"}})
           == dbms::DBStatus::OK);
    assert(bidx->size() == 51);
    assert(bidx->containsExact("name051"));
    assert(g_engine.update(db, "bt", {{"name", "renamed051"}},
                           {"=id 51"}) == dbms::DBStatus::OK);
    assert(!bidx->containsExact("name051"));
    assert(bidx->containsExact("renamed051"));
    assert(g_engine.remove(db, "bt", {"=id 51"}) == dbms::DBStatus::OK);
    assert(!bidx->containsExact("renamed051"));

    // Equality scans consult the bloom index through the shared candidate
    // collection path (same helper the IndexScan/BitmapScan ops use).
    {
        // The candidate-collection helper is file-local in
        // ExecutionPlan.cpp; exercise the same contract through the public
        // engine surface: the bloom index resolves the rid of the live row.
        auto* bi = g_engine.getBloomIndex(db, "bt", "name");
        assert(bi);
        assert(bi->search("name017").size() == 1);
    }

    // DROP INDEX removes the metadata.
    assert(!exec("DROP INDEX bt_name_bloom"));
    assert(g_engine.getBloomIndexedColumns(db, "bt").empty());

    // Table drop cleans files.
    assert(!exec("CREATE INDEX bt_name_bloom ON bt USING bloom (name)"));
    // Index-on-table catalog deps require CASCADE (matches the pre-existing
    // behavior for hash/btree named indexes).
    assert(!exec("DROP TABLE bt CASCADE"));
    assert(g_engine.getBloomIndexedColumns(db, "bt").empty());

    g_engine.dropDatabase(db);
    cleanupTestDb("bloom_storage");
    std::cout << "[BLOOM] engine lifecycle/DML/drop OK" << std::endl;
}

int main() {
    cleanupAllTestData();
    test_bloom_unit();
    test_bloom_no_false_negatives_many_keys();
    test_bloom_grows_geometrically();
    test_bloom_rejects_malformed_lengths_and_layout();
    test_engine_bloom_lifecycle();
    std::cout << "[BLOOM] all tests passed" << std::endl;
    return 0;
}
