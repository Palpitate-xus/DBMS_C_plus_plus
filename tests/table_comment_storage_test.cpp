#include "Config.h"
#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

static TableSchema makeTable(const std::string& name,
                             const std::string& textColumn = "body") {
    TableSchema table;
    table.tablename = name;
    table.append(makeIntColumn("id", false, 0, true));
    table.append(makeVarCharColumn(textColumn, true, 128));
    return table;
}

static void testRoundTripAndValidation(StorageEngine& engine) {
    const std::string database = testDbPath("table_comments");
    cleanupTestDb("table_comments");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("notes")) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("other")) == DBStatus::OK);

    const fs::path comments = fs::path(database) / ".comments";
    assert(engine.commentOnColumn(database, "notes", "missing", "orphan") ==
           DBStatus::INVALID_VALUE);
    assert(!fs::exists(comments));

    std::string tableComment =
        "first line|delimiter\nT|other|forged\\tail\r";
    tableComment.push_back('\0');
    tableComment += "last";
    const std::string columnComment =
        "column line one\nC|other|body|forged";
    assert(engine.commentOnTable(database, "notes", tableComment) ==
           DBStatus::OK);
    assert(engine.commentOnColumn(database, "notes", "body", columnComment) ==
           DBStatus::OK);
    assert(engine.commentOnTable(database, "other", "real comment") ==
           DBStatus::OK);

    assert(engine.getTableComment(database, "notes") == tableComment);
    assert(engine.getColumnComment(database, "notes", "body") ==
           columnComment);
    assert(engine.getTableComment(database, "other") == "real comment");

    std::ifstream stored(comments, std::ios::binary);
    assert(stored);
    size_t recordCount = 0;
    std::string line;
    while (std::getline(stored, line)) {
        assert(line.rfind("T2|", 0) == 0 || line.rfind("C2|", 0) == 0);
        assert(line.find("forged") == std::string::npos);
        ++recordCount;
    }
    assert(recordCount == 3);

    assert(engine.commentOnTable(database, "notes", "") == DBStatus::OK);
    assert(engine.getTableComment(database, "notes").empty());
    assert(engine.getColumnComment(database, "notes", "body") ==
           columnComment);

    assert(engine.createTable(database,
                              makeTable("odd|table", "odd|column")) ==
           DBStatus::OK);
    assert(engine.commentOnTable(database, "odd|table", "odd table") ==
           DBStatus::OK);
    assert(engine.commentOnColumn(database, "odd|table", "odd|column",
                                  "odd column") == DBStatus::OK);
    assert(engine.getTableComment(database, "odd|table") == "odd table");
    assert(engine.getColumnComment(database, "odd|table", "odd|column") ==
           "odd column");

    cleanupTestDb("table_comments");
}

static void testLegacyRecords(StorageEngine& engine) {
    const std::string database = testDbPath("legacy_table_comments");
    cleanupTestDb("legacy_table_comments");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("legacy")) == DBStatus::OK);

    const fs::path comments = fs::path(database) / ".comments";
    {
        std::ofstream out(comments, std::ios::binary | std::ios::trunc);
        assert(out);
        out << "T|legacy|old table comment\n";
        out << "C|legacy|body|old column comment\n";
        assert(out);
    }
    assert(engine.getTableComment(database, "legacy") ==
           "old table comment");
    assert(engine.getColumnComment(database, "legacy", "body") ==
           "old column comment");

    assert(engine.commentOnTable(database, "legacy", "new table comment") ==
           DBStatus::OK);
    assert(engine.commentOnColumn(database, "legacy", "body",
                                  "new column comment") == DBStatus::OK);
    assert(engine.getTableComment(database, "legacy") ==
           "new table comment");
    assert(engine.getColumnComment(database, "legacy", "body") ==
           "new column comment");

    std::ifstream stored(comments, std::ios::binary);
    assert(stored);
    std::string line;
    while (std::getline(stored, line)) {
        assert(line.rfind("T2|", 0) == 0 || line.rfind("C2|", 0) == 0);
    }

    cleanupTestDb("legacy_table_comments");
}

static void testIoFailures(StorageEngine& engine) {
    const std::string database = testDbPath("table_comment_io");
    cleanupTestDb("table_comment_io");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("notes")) == DBStatus::OK);

    const fs::path comments = fs::path(database) / ".comments";
    assert(fs::create_directory(comments));
    assert(engine.commentOnTable(database, "notes", "must fail") ==
           DBStatus::IO_ERROR);
    assert(engine.commentOnColumn(database, "notes", "body", "must fail") ==
           DBStatus::IO_ERROR);
    assert(fs::is_directory(comments));

    cleanupTestDb("table_comment_io");
}

static bool hasColumn(const TableSchema& table, const std::string& name) {
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == name) return true;
    }
    return false;
}

static void testRenameAndDropLifecycle(StorageEngine& engine) {
    const std::string database = testDbPath("table_comment_lifecycle");
    cleanupTestDb("table_comment_lifecycle");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("source")) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("unrelated")) ==
           DBStatus::OK);
    assert(engine.commentOnTable(database, "source", "live table") ==
           DBStatus::OK);
    assert(engine.commentOnColumn(database, "source", "body", "live column") ==
           DBStatus::OK);
    assert(engine.commentOnTable(database, "unrelated", "keep me") ==
           DBStatus::OK);

    // Simulate records left under a reusable destination name by an older
    // engine version.  Rename must discard them before moving live metadata.
    {
        std::ofstream stale(fs::path(database) / ".comments",
                            std::ios::binary | std::ios::app);
        assert(stale);
        stale << "T|renamed|stale table\n";
        stale << "C|renamed|renamed_body|stale column\n";
        assert(stale);
    }

    assert(engine.alterTableRenameColumn(
               database, "source", "body", "renamed_body") == DBStatus::OK);
    assert(engine.getColumnComment(database, "source", "body").empty());
    assert(engine.getColumnComment(
               database, "source", "renamed_body") == "live column");

    assert(engine.alterTableRenameTable(database, "source", "renamed") ==
           DBStatus::OK);
    assert(engine.getTableComment(database, "source").empty());
    assert(engine.getColumnComment(
               database, "source", "renamed_body").empty());
    assert(engine.getTableComment(database, "renamed") == "live table");
    assert(engine.getColumnComment(
               database, "renamed", "renamed_body") == "live column");
    assert(engine.getTableComment(database, "unrelated") == "keep me");

    assert(engine.dropTable(database, "renamed") == DBStatus::OK);
    assert(engine.getTableComment(database, "renamed").empty());
    assert(engine.getColumnComment(
               database, "renamed", "renamed_body").empty());
    assert(engine.getTableComment(database, "unrelated") == "keep me");

    assert(engine.createTable(
               database, makeTable("renamed", "renamed_body")) ==
           DBStatus::OK);
    assert(engine.getTableComment(database, "renamed").empty());
    assert(engine.getColumnComment(
               database, "renamed", "renamed_body").empty());

    cleanupTestDb("table_comment_lifecycle");
}

static void testLifecycleMetadataFailures(StorageEngine& engine) {
    const std::string database = testDbPath("table_comment_lifecycle_io");
    cleanupTestDb("table_comment_lifecycle_io");
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, makeTable("source")) == DBStatus::OK);
    const fs::path comments = fs::path(database) / ".comments";
    assert(fs::create_directory(comments));

    assert(engine.alterTableRenameColumn(
               database, "source", "body", "renamed_body") ==
           DBStatus::IO_ERROR);
    TableSchema table = engine.getTableSchema(database, "source");
    assert(hasColumn(table, "body"));
    assert(!hasColumn(table, "renamed_body"));

    assert(engine.alterTableRenameTable(database, "source", "renamed") ==
           DBStatus::IO_ERROR);
    assert(engine.tableExists(database, "source"));
    assert(!engine.tableExists(database, "renamed"));

    assert(engine.dropTable(database, "source") == DBStatus::IO_ERROR);
    assert(engine.tableExists(database, "source"));

    cleanupTestDb("table_comment_lifecycle_io");
}

int main() {
    cleanupAllTestData();
    StorageEngine engine;
    testRoundTripAndValidation(engine);
    testLegacyRecords(engine);
    testIoFailures(engine);
    testRenameAndDropLifecycle(engine);
    testLifecycleMetadataFailures(engine);
    finalCleanupTestData();
    std::cout << "[TABLE COMMENT STORAGE] all passed\n";
    return 0;
}
