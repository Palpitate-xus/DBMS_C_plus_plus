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

int main() {
    cleanupAllTestData();
    StorageEngine engine;
    testRoundTripAndValidation(engine);
    testLegacyRecords(engine);
    testIoFailures(engine);
    finalCleanupTestData();
    std::cout << "[TABLE COMMENT STORAGE] all passed\n";
    return 0;
}
