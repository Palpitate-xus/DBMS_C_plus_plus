#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <map>

using namespace dbms;

static std::string payload(uint32_t seed) {
    static constexpr char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string result(10000, '\0');
    for (char& value : result) {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        value = alphabet[seed % (sizeof(alphabet) - 1)];
    }
    return result;
}

static void assertPayloads(StorageEngine& engine, const std::string& db,
                           const std::map<std::string, std::string>& expected) {
    const auto ids = engine.query(db, "items", {}, {"id"});
    if (ids.size() != expected.size())
        std::cerr << "[SHARED TOAST] expected_rows=" << expected.size()
                  << " actual_rows=" << ids.size() << '\n';
    assert(ids.size() == expected.size());
    for (const auto& [id, data] : expected) {
        const auto rows = engine.query(db, "items", {"=id " + id}, {"payload"});
        assert(rows.size() == 1);
        if (rows.front() != data + " ")
            std::cerr << "[SHARED TOAST] id=" << id << " expected_bytes="
                      << data.size() << " actual_bytes=" << rows.front().size() << '\n';
        assert(rows.front() == data + " ");
    }
}

int main() {
    const std::string db = "__t_shared_btree_toast";
    const std::map<std::string, std::string> expected{
        {"1", payload(123)}, {"99", payload(456)}, {"100", payload(789)}};
    {
        StorageEngine first, second;
        assert(first.createDatabase(db) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.pkColIndices.push_back(0);
        table.append(makeVarCharColumn("payload", false, 12000));
        assert(first.createTable(db, table) == DBStatus::OK);
        assert(first.insert(db, "items", {{"id", "1"}, {"payload", expected.at("1")}}) == DBStatus::OK);
        // Reading the seed warms the peer's TOAST tree before both writers.
        assertPayloads(second, db, {{"1", expected.at("1")}});
        assert(first.beginTransaction(db) == DBStatus::OK);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(first.insert(db, "items", {{"id", "99"}, {"payload", expected.at("99")}}) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "100"}, {"payload", expected.at("100")}}) == DBStatus::OK);
        assert(first.commitTransaction() == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        assertPayloads(first, db, expected);
        assertPayloads(second, db, expected);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "101"}, {"payload", payload(987)}}) == DBStatus::OK);
        assert(second.rollbackTransaction() == DBStatus::OK);
        assertPayloads(first, db, expected);
        assertPayloads(second, db, expected);
    }
    StorageEngine reopened;
    assertPayloads(reopened, db, expected);
    std::cout << "[SHARED TOAST] every committed external payload survives both caches and reopen\n";
}
