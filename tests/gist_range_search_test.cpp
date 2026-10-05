#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/GiSTIndexFormat.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <vector>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "gist_range_search_db";
constexpr const char* kTable = "values_table";

void cleanup() {
    std::error_code error;
    fs::remove_all(kDatabase, error);
    error.clear();
    fs::remove_all("info/.prepared", error);
    error.clear();
    fs::remove(".txnid", error);
}

int64_t ridFor(StorageEngine& engine, const std::string& id) {
    BPTree* index = engine.getPKIndex(kDatabase, kTable);
    assert(index != nullptr);
    int64_t rid = -1;
    assert(index->search(id, rid));
    return rid;
}

void assertRids(const std::vector<int64_t>& actual,
                std::initializer_list<int64_t> expected) {
    assert(std::set<int64_t>(actual.begin(), actual.end()) ==
           std::set<int64_t>(expected.begin(), expected.end()));
}

void buildFixture(StorageEngine& engine) {
    assert(engine.createDatabase(kDatabase) == DBStatus::OK);
    TableSchema table;
    table.tablename = kTable;
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeIntColumn("number_value", false, 4, false));
    table.append(makeVarCharColumn("text_value", false, 128, false));
    table.append(makeDecimalColumn("exact_value", false, 30, 1, false));
    assert(engine.createTable(kDatabase, table) == DBStatus::OK);

    const std::vector<std::map<std::string, std::string>> rows = {
        {{"id", "1"}, {"number_value", "-5"}, {"text_value", "2"},
         {"exact_value", "9007199254740992.1"}},
        {{"id", "2"}, {"number_value", "2"}, {"text_value", "10"},
         {"exact_value", "9007199254740992.2"}},
        {{"id", "3"}, {"number_value", "10"},
         {"text_value", "hello 世界"},
         {"exact_value", "9007199254740992.3"}},
        {{"id", "4"}, {"number_value", "100"}, {"text_value", "alpha"},
         {"exact_value", "9007199254740992.4"}},
        {{"id", "5"}, {"number_value", "1000"}, {"text_value", "zulu"},
         {"exact_value", "9007199254740992.5"}},
    };
    for (const auto& row : rows) {
        assert(engine.insert(kDatabase, kTable, row) == DBStatus::OK);
    }
    assert(engine.createGiSTIndex(
               kDatabase, kTable, "number_value") == DBStatus::OK);
    assert(engine.createGiSTIndex(
               kDatabase, kTable, "text_value") == DBStatus::OK);
    assert(engine.createGiSTIndex(
               kDatabase, kTable, "exact_value") == DBStatus::OK);
}

void testTypedRangeSearch(StorageEngine& engine) {
    const int64_t rid1 = ridFor(engine, "1");
    const int64_t rid2 = ridFor(engine, "2");
    const int64_t rid3 = ridFor(engine, "3");
    const int64_t rid4 = ridFor(engine, "4");
    const int64_t rid5 = ridFor(engine, "5");

    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "50", "200"),
               {rid4});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "10", "10"),
               {rid3});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "180", ""),
               {rid5});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "180", "\x7f"),
               {rid5});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "", "0"),
               {rid1});
    assert(engine.giSTSearchOverlap(
               kDatabase, kTable, "number_value", "200", "50").empty());
    assertRids(engine.giSTSearchContainedBy(
                   kDatabase, kTable, "number_value", "2", "100"),
               {rid2, rid3, rid4});

    // Numeric-looking text must retain lexical semantics, while NUMERIC must
    // retain precision beyond IEEE-754 double's exact integer range.
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "text_value", "10", "10"),
               {rid2});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "text_value", "hello 世界",
                   "hello 世界"),
               {rid3});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "text_value", "hello ",
                   "hello \x7f"),
               {rid3});
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "exact_value",
                   "9007199254740992.2", "9007199254740992.2"),
               {rid2});
    assert(engine.giSTSearchOverlap(
               kDatabase, kTable, "exact_value", "NaN", "NaN").empty());

    std::cout << "[GIST RANGE] typed/open/exact comparisons OK\n";
}

void testCorruptSidecarFallback(StorageEngine& engine) {
    const fs::path path =
        fs::path(kDatabase) / "values_table_number_value.gist";
    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output << "not a valid gist record\n";
        assert(output.good());
    }
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "50", "200"),
               {ridFor(engine, "4")});
    assertRids(engine.giSTSearchContainedBy(
                   kDatabase, kTable, "number_value", "2", "100"),
               {ridFor(engine, "2"), ridFor(engine, "3"),
                ridFor(engine, "4")});
    std::cout << "[GIST RANGE] corrupt sidecar heap fallback OK\n";
}

void testV2ChecksumFallback(StorageEngine& engine) {
    const fs::path path =
        fs::path(kDatabase) / "values_table_number_value.gist";
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    std::string bytes((std::istreambuf_iterator<char>(input)),
                      std::istreambuf_iterator<char>());
    assert(gist_index_format::hasMagic(bytes));
    std::vector<gist_index_format::Entry> entries;
    assert(gist_index_format::decodeV2(bytes, entries));
    assert(entries.size() == 5);

    // Row 2's two bounds are both changed from 2 to 3, leaving a structurally
    // valid range. The CRC must be what rejects this otherwise plausible
    // false-negative source.
    size_t secondEntryLow = gist_index_format::kHeaderBytes;
    bool foundSecondRow = false;
    for (const auto& entry : entries) {
        if (entry.rid == static_cast<uint64_t>(ridFor(engine, "2"))) {
            assert(entry.low == "2" && entry.high == "2");
            foundSecondRow = true;
            break;
        }
        secondEntryLow += gist_index_format::kMinimumEntryBytes +
                          entry.low.size() + entry.high.size();
    }
    assert(foundSecondRow);
    secondEntryLow += gist_index_format::kMinimumEntryBytes;
    assert(bytes.at(secondEntryLow) == '2');
    assert(bytes.at(secondEntryLow + 1) == '2');
    bytes[secondEntryLow] = '3';
    bytes[secondEntryLow + 1] = '3';
    std::vector<gist_index_format::Entry> damagedEntries;
    assert(!gist_index_format::decodeV2(bytes, damagedEntries));
    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        assert(output.good());
    }

    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "2", "2"),
               {ridFor(engine, "2")});
    std::cout << "[GIST RANGE] checksummed plausible-corruption fallback OK\n";
}

void testLegacyCompatibility(StorageEngine& engine) {
    const fs::path path =
        fs::path(kDatabase) / "values_table_number_value.gist";
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    for (const auto& row : std::vector<std::pair<std::string, std::string>>{
             {"1", "-5"}, {"2", "2"}, {"3", "10"},
             {"4", "100"}, {"5", "1000"}}) {
        output << ridFor(engine, row.first) << ' ' << std::quoted(row.second)
               << ' ' << std::quoted(row.second) << '\n';
    }
    output.close();
    assert(output.good());
    assertRids(engine.giSTSearchOverlap(
                   kDatabase, kTable, "number_value", "2", "2"),
               {ridFor(engine, "2")});
    std::cout << "[GIST RANGE] legacy sidecar compatibility OK\n";
}

}  // namespace

int main() {
    cleanup();
    TypeRegistry::instance().bootstrap();
    {
        StorageEngine engine;
        buildFixture(engine);
        testTypedRangeSearch(engine);
        testV2ChecksumFallback(engine);
        testLegacyCompatibility(engine);
        testCorruptSidecarFallback(engine);
    }
    cleanup();
    std::cout << "[GIST RANGE] all tests passed\n";
    return 0;
}
