#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include <cassert>
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& p) {
    if (fs::exists(p)) fs::remove(p);
}

static void test_hash_persistence_and_corruption() {
    std::string idx = "/tmp/hash_index_test.idx";
    cleanup(idx);
    {
        dbms::HashIndex hash(idx);
        assert(hash.open());
        hash.insert("alpha", 10);
        hash.insert("alpha", 11);
        assert(hash.flush());
        assert(hash.close());
    }
    {
        dbms::HashIndex hash(idx);
        assert(hash.open());
        assert((hash.search("alpha") == std::vector<int64_t>{10, 11}));
        assert(hash.close());
    }
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        out << "truncated index";
    }
    dbms::HashIndex broken(idx);
    assert(!broken.open());
    cleanup(idx);
    std::cout << "[HASH] durable persistence/corruption rejection OK" << std::endl;
}

static void test_storage_engine_gin_brin() {
    const std::string db = testDbPath("gin_brin_storage");
    cleanupTestDb("gin_brin_storage");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.currentDB = db;
    session.username = "testuser";
    session.permission = 1;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, value TEXT)", session));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"value", "hello world"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"value", "world peace"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"value", "hello dbms"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE INDEX t_value_gin ON t USING GIN (value)", session));
    assert(g_engine.ginSearch(db, "t", "value", "hello").size() == 2);
    assert(g_engine.ginSearch(db, "t", "value", "world").size() == 2);
    assert(g_engine.ginSearch(db, "t", "value", "missing").empty());

    assert(!ddl.executeSql("CREATE INDEX t_id_brin ON t USING BRIN (id)", session));
    assert(!g_engine.brinSearchRange(db, "t", "id", "=", "1").empty());
    assert(g_engine.brinSearchRange(db, "t", "id", "=", "99").empty());

    // The production search paths must reject a damaged sidecar rather than
    // treating it as an empty, valid index.
    {
        std::ofstream broken(db + "/t_value.gin", std::ios::trunc);
        broken << "broken posting not-a-row-id\n";
    }
    assert(g_engine.ginSearch(db, "t", "value", "hello").empty());
    {
        std::ofstream broken(db + "/t_id.brin", std::ios::binary | std::ios::trunc);
        broken << "truncated brin";
    }
    assert(g_engine.brinSearchRange(db, "t", "id", "=", "1").empty());

    cleanupTestDb("gin_brin_storage");
    std::cout << "[GIN/BRIN] StorageEngine canonical paths and corruption rejection OK" << std::endl;
}

static void test_btree_split_and_range() {
    std::string idx = "/tmp/btree_split_test.idx";
    cleanup(idx);
    {
        dbms::BPTree tree(idx);
        assert(tree.open());

        // BP_KEY_LEN is 20 and the default leaf order is 100.  This crosses
        // the root-leaf split boundary and keeps keys lexicographically
        // ordered without relying on numeric-string ordering.
        for (int i = 0; i < 250; ++i) {
            char key[16];
            std::snprintf(key, sizeof(key), "k%04d", i);
            assert(tree.insert(key, i));
        }

        for (int i = 0; i < 250; ++i) {
            char key[16];
            std::snprintf(key, sizeof(key), "k%04d", i);
            int64_t value = -1;
            assert(tree.search(key, value));
            assert(value == i);
        }

        auto range = tree.rangeScan("k0050", "k0100");
        assert(range.size() == 51);
        auto all = tree.allValues();
        assert(all.size() == 250);
        tree.close();
    }
    cleanup(idx);

    // Duplicate secondary-index keys must remain discoverable when equal
    // keys span more than one leaf page.
    {
        dbms::BPTree tree(idx);
        assert(tree.open());
        // Enough rows to force both leaf and internal-node splits.
        for (int i = 0; i < 6000; ++i) assert(tree.insertMulti("same", i));
        auto matches = tree.searchMulti("same");
        assert(matches.size() == 6000);
        assert(tree.removeMulti("same", 3000));
        assert(!tree.removeMulti("same", 3000));
        matches = tree.searchMulti("same");
        assert(matches.size() == 5999);
        assert(std::find(matches.begin(), matches.end(), 2999) != matches.end());
        assert(std::find(matches.begin(), matches.end(), 3001) != matches.end());
        assert(tree.flush());
        tree.close();
        dbms::BPTree reopened(idx);
        assert(reopened.open());
        assert(reopened.searchMulti("same").size() == 5999);
        reopened.close();
    }
    cleanup(idx);

    // Every public key operation must use the same fixed-width representation
    // after reopening.  This also verifies that range endpoints are inclusive
    // and that a key longer than BP_KEY_LEN follows the documented truncation
    // rule instead of becoming an unreachable entry.
    {
        dbms::BPTree tree(idx);
        assert(tree.open());
        const std::string longKey = "01234567890123456789-truncated";
        assert(tree.insertMulti(longKey, 7));
        int64_t value = -1;
        assert(tree.search(longKey, value));
        assert(value == 7);
        assert(tree.search("01234567890123456789", value));
        assert(value == 7);
        assert(tree.rangeScan(longKey, longKey).size() == 1);
        assert(tree.removeMulti(longKey, 7));
        assert(tree.searchMulti(longKey).empty());
        assert(tree.flush());
        tree.close();

        dbms::BPTree reopened(idx);
        assert(reopened.open());
        assert(reopened.insertMulti(longKey, 8));
        assert(reopened.searchMulti("01234567890123456789").size() == 1);
        reopened.close();
    }
    cleanup(idx);
    std::cout << "[BPTREE] root split/search/range OK" << std::endl;
}

int main() {
    test_hash_persistence_and_corruption();
    test_storage_engine_gin_brin();
    test_btree_split_and_range();
    std::cout << "[ACCESS] all passed" << std::endl;
    return 0;
}
