#include "Config.h"
#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <tuple>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

static TableSchema makeTable(const std::string& name) {
    TableSchema table;
    table.tablename = name;
    table.append(makeIntColumn("id", false, 0, true));
    table.append(makeVarCharColumn("body", true, 128));
    return table;
}

static bool containsLabel(
    const std::vector<std::tuple<std::string, std::string, std::string>>& labels,
    const std::string& type, const std::string& name,
    const std::string& label) {
    for (const auto& entry : labels) {
        if (std::get<0>(entry) == type && std::get<1>(entry) == name &&
            std::get<2>(entry) == label) {
            return true;
        }
    }
    return false;
}

static void testRoundTripAndValidation(StorageEngine& engine) {
    const std::string database = testDbPath("security_labels");
    cleanupTestDb("security_labels");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("docs")) == DBStatus::OK);
    const fs::path labelsPath = fs::path(database) / ".security_labels";

    assert(engine.setSecurityLabel(
               "missing_database", "table", "docs", "label") ==
           DBStatus::DATABASE_NOT_FOUND);
    assert(engine.setSecurityLabel(
               database, "table", "missing", "label") ==
           DBStatus::TABLE_NOT_FOUND);
    assert(engine.setSecurityLabel(
               database, "column", "docs", "label") ==
           DBStatus::INVALID_ARGUMENT);
    assert(engine.setSecurityLabel(
               database, "column", "docs.missing", "label") ==
           DBStatus::INVALID_VALUE);
    assert(!fs::exists(labelsPath));

    std::string tableLabel = "system_u:object_r:data_t:s0|pipe\n";
    tableLabel.push_back('\0');
    tableLabel += "tail";
    const std::string columnLabel =
        "column label\nS2|forged|record|value";
    assert(engine.setSecurityLabel(
               database, "table", "docs", tableLabel) == DBStatus::OK);
    assert(engine.setSecurityLabel(
               database, "column", "docs.body", columnLabel) ==
           DBStatus::OK);
    assert(engine.setSecurityLabel(
               database, "role", "role with spaces", "role label") ==
           DBStatus::OK);

    assert(engine.getSecurityLabel(database, "table", "docs") == tableLabel);
    assert(engine.getSecurityLabel(
               database, "column", "docs.body") == columnLabel);
    assert(engine.getSecurityLabel(
               database, "role", "role with spaces") == "role label");
    const auto allLabels = engine.getAllSecurityLabels(database);
    assert(allLabels.size() == 3);
    assert(containsLabel(allLabels, "table", "docs", tableLabel));
    assert(containsLabel(
        allLabels, "column", "docs.body", columnLabel));
    assert(containsLabel(
        allLabels, "role", "role with spaces", "role label"));

    std::ifstream stored(labelsPath, std::ios::binary);
    assert(stored);
    size_t records = 0;
    std::string line;
    while (std::getline(stored, line)) {
        assert(line.rfind("S2|", 0) == 0);
        assert(line.find("forged") == std::string::npos);
        ++records;
    }
    assert(records == 3);

    assert(engine.setSecurityLabel(database, "table", "docs", "") ==
           DBStatus::OK);
    assert(engine.getSecurityLabel(database, "table", "docs").empty());
    assert(engine.getAllSecurityLabels(database).size() == 2);

    cleanupTestDb("security_labels");
}

static void testLegacyMigration(StorageEngine& engine) {
    const std::string database = testDbPath("legacy_security_labels");
    cleanupTestDb("legacy_security_labels");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("docs")) == DBStatus::OK);
    const fs::path labelsPath = fs::path(database) / ".security_labels";
    {
        std::ofstream legacy(labelsPath, std::ios::binary | std::ios::trunc);
        assert(legacy);
        legacy << "table docs old table label\n";
        legacy << "column docs.body old column label\n";
        assert(legacy);
    }
    assert(engine.getSecurityLabel(database, "table", "docs") ==
           "old table label");
    assert(engine.getSecurityLabel(database, "column", "docs.body") ==
           "old column label");

    assert(engine.setSecurityLabel(
               database, "table", "docs", "new table label") ==
           DBStatus::OK);
    assert(engine.getSecurityLabel(database, "table", "docs") ==
           "new table label");
    assert(engine.getSecurityLabel(database, "column", "docs.body") ==
           "old column label");

    std::ifstream stored(labelsPath, std::ios::binary);
    assert(stored);
    std::string line;
    while (std::getline(stored, line)) {
        assert(line.rfind("S2|", 0) == 0);
    }
    cleanupTestDb("legacy_security_labels");
}

static void testStorageFailures(StorageEngine& engine) {
    const std::string corruptDatabase =
        testDbPath("corrupt_security_labels");
    cleanupTestDb("corrupt_security_labels");
    assert(engine.createDatabase(corruptDatabase) == DBStatus::OK);
    assert(engine.createTable(corruptDatabase, makeTable("docs")) ==
           DBStatus::OK);
    const fs::path corruptPath =
        fs::path(corruptDatabase) / ".security_labels";
    {
        std::ofstream corrupt(corruptPath, std::ios::binary | std::ios::trunc);
        assert(corrupt);
        corrupt << "malformed record\n";
    }
    assert(engine.setSecurityLabel(
               corruptDatabase, "table", "docs", "must fail") ==
           DBStatus::CORRUPTED_DATA);
    std::ifstream unchanged(corruptPath, std::ios::binary);
    assert(unchanged);
    const std::string bytes((std::istreambuf_iterator<char>(unchanged)),
                            std::istreambuf_iterator<char>());
    assert(bytes == "malformed record\n");
    cleanupTestDb("corrupt_security_labels");

    const std::string ioDatabase = testDbPath("security_label_io");
    cleanupTestDb("security_label_io");
    assert(engine.createDatabase(ioDatabase) == DBStatus::OK);
    assert(engine.createTable(ioDatabase, makeTable("docs")) == DBStatus::OK);
    const fs::path ioPath = fs::path(ioDatabase) / ".security_labels";
    assert(fs::create_directory(ioPath));
    assert(engine.setSecurityLabel(
               ioDatabase, "table", "docs", "must fail") ==
           DBStatus::IO_ERROR);
    assert(fs::is_directory(ioPath));
    cleanupTestDb("security_label_io");
}

int main() {
    cleanupAllTestData();
    StorageEngine engine;
    testRoundTripAndValidation(engine);
    testLegacyMigration(engine);
    testStorageFailures(engine);
    finalCleanupTestData();
    std::cout << "[SECURITY LABEL STORAGE] all passed\n";
    return 0;
}
