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

static void testWritesFailClosedWithoutProvider(StorageEngine& engine) {
    const std::string database = testDbPath("security_labels");
    cleanupTestDb("security_labels");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("docs")) == DBStatus::OK);
    assert(sqlstateForDBStatus(DBStatus::FEATURE_NOT_SUPPORTED) == "0A000");
    const fs::path labelsPath = fs::path(database) / ".security_labels";

    assert(engine.setSecurityLabel(
               "missing_database", "table", "docs", "label") ==
           DBStatus::DATABASE_NOT_FOUND);
    assert(engine.setSecurityLabel(
               database, "table", "missing", "label") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.setSecurityLabel(
               database, "column", "docs", "label") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.setSecurityLabel(
               database, "column", "docs.missing", "label") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(!fs::exists(labelsPath));

    assert(engine.setSecurityLabel(
               database, "table", "docs", "inert policy") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.setSecurityLabel(
               database, "column", "docs.body", "inert policy") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.setSecurityLabel(
               database, "role", "role with spaces", "role label") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(!fs::exists(labelsPath));

    assert(engine.setSecurityLabel(database, "table", "docs", "") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.getSecurityLabel(database, "table", "docs").empty());
    assert(engine.getAllSecurityLabels(database).empty());

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
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.getSecurityLabel(database, "table", "docs") ==
           "old table label");
    assert(engine.getSecurityLabel(database, "column", "docs.body") ==
           "old column label");

    std::ifstream stored(labelsPath, std::ios::binary);
    assert(stored);
    const std::string bytes((std::istreambuf_iterator<char>(stored)),
                            std::istreambuf_iterator<char>());
    assert(bytes == "table docs old table label\n"
                    "column docs.body old column label\n");
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
           DBStatus::FEATURE_NOT_SUPPORTED);
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
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(fs::is_directory(ioPath));
    cleanupTestDb("security_label_io");
}

static bool hasColumn(const TableSchema& table, const std::string& name) {
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == name) return true;
    }
    return false;
}

static void testRenameAndDropLifecycle(StorageEngine& engine) {
    const std::string database = testDbPath("security_label_lifecycle");
    cleanupTestDb("security_label_lifecycle");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("source")) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("unrelated")) ==
           DBStatus::OK);
    // Seed legacy metadata directly. New writes are refused without a loaded
    // provider, but old rows still have to follow rename/drop lifecycle so
    // they cannot become attached to a later object with the same name.
    {
        std::ofstream stale(fs::path(database) / ".security_labels",
                            std::ios::binary | std::ios::trunc);
        assert(stale);
        stale << "table source live table\n";
        stale << "column source.body live column\n";
        stale << "table unrelated keep me\n";
        stale << "table renamed stale table\n";
        stale << "column renamed.renamed_body stale column\n";
        assert(stale);
    }

    assert(engine.alterTableRenameColumn(
               database, "source", "body", "renamed_body") == DBStatus::OK);
    assert(engine.getSecurityLabel(
               database, "column", "source.body").empty());
    assert(engine.getSecurityLabel(
               database, "column", "source.renamed_body") == "live column");

    assert(engine.alterTableRenameTable(database, "source", "renamed") ==
           DBStatus::OK);
    assert(engine.getSecurityLabel(database, "table", "source").empty());
    assert(engine.getSecurityLabel(
               database, "column", "source.renamed_body").empty());
    assert(engine.getSecurityLabel(
               database, "table", "renamed") == "live table");
    assert(engine.getSecurityLabel(
               database, "column", "renamed.renamed_body") == "live column");
    assert(engine.getSecurityLabel(
               database, "table", "unrelated") == "keep me");

    assert(engine.dropTable(database, "renamed") == DBStatus::OK);
    assert(engine.getSecurityLabel(database, "table", "renamed").empty());
    assert(engine.getSecurityLabel(
               database, "column", "renamed.renamed_body").empty());
    assert(engine.getSecurityLabel(
               database, "table", "unrelated") == "keep me");

    assert(engine.createTable(database, makeTable("renamed")) == DBStatus::OK);
    assert(engine.getSecurityLabel(database, "table", "renamed").empty());
    assert(engine.getSecurityLabel(
               database, "column", "renamed.body").empty());
    cleanupTestDb("security_label_lifecycle");
}

static void testLifecycleMetadataFailures(StorageEngine& engine) {
    const std::string database = testDbPath("security_label_lifecycle_io");
    cleanupTestDb("security_label_lifecycle_io");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("source")) == DBStatus::OK);
    const fs::path labelsPath = fs::path(database) / ".security_labels";
    assert(fs::create_directory(labelsPath));

    assert(engine.alterTableRenameColumn(
               database, "source", "body", "renamed_body") ==
           DBStatus::IO_ERROR);
    const TableSchema table = engine.getTableSchema(database, "source");
    assert(hasColumn(table, "body"));
    assert(!hasColumn(table, "renamed_body"));

    assert(engine.alterTableRenameTable(database, "source", "renamed") ==
           DBStatus::IO_ERROR);
    assert(engine.tableExists(database, "source"));
    assert(!engine.tableExists(database, "renamed"));

    assert(engine.dropTable(database, "source") == DBStatus::IO_ERROR);
    assert(engine.tableExists(database, "source"));
    cleanupTestDb("security_label_lifecycle_io");
}

int main() {
    cleanupAllTestData();
    StorageEngine engine;
    testWritesFailClosedWithoutProvider(engine);
    testLegacyMigration(engine);
    testStorageFailures(engine);
    testRenameAndDropLifecycle(engine);
    testLifecycleMetadataFailures(engine);
    finalCleanupTestData();
    std::cout << "[SECURITY LABEL STORAGE] all passed\n";
    return 0;
}
