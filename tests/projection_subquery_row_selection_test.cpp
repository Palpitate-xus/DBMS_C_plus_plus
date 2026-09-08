#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "projection_subquery_row_selection";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    for (const std::string tableName : {"outer_rows", "inner_rows"}) {
        dbms::TableSchema table;
        table.tablename = tableName;
        table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        table.append(dbms::makeIntColumn("id", false, 4, true));
        table.append(dbms::makeTextColumn("payload", false));
        table.append(dbms::makeIntColumn("rank", true, 4));
        assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
        assert(g_engine.insertRow(db, tableName,
            {{"id", "1"}, {"payload", "10"}, {"rank", "2"}}) == dbms::DBStatus::OK);
    }
    assert(g_engine.insertRow(db, "inner_rows",
        {{"id", "2"}, {"payload", "2"}, {"rank", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "inner_rows",
        {{"id", "3"}, {"payload", "30"}, {"rank", std::nullopt}}) == dbms::DBStatus::OK);
    auto project = [&](const std::string& sql, bool exists = false, bool negate = false) {
        dbms::StorageEngine::SelectExpr expr;
        expr.displayName = "value";
        expr.isScalar = true;
        expr.funcName = exists ? "exists_sub" : "subquery";
        expr.funcArgs = {sql};
        if (negate) expr.funcArgs.push_back("not");
        return g_engine.queryExpr(db, "outer_rows", {}, {expr});
    };
    auto scalar = [&](const std::string& sql, const std::string& value) {
        const auto rows = project(sql);
        if (rows != std::vector<std::string>{value + " "}) {
            std::cerr << sql << " expected [" << value << "] got";
            for (const auto& row : rows) std::cerr << " [" << row << ']';
            std::cerr << '\n';
        }
        assert(rows == std::vector<std::string>{value + " "});
    };
    scalar("select id from inner_rows order by id desc limit 1", "3");
    scalar("select id from inner_rows limit 1 offset 1", "2");
    scalar("select id from inner_rows order by id limit 1 offset 2", "3");
    scalar("select id from inner_rows limit all offset 2", "3");
    scalar("select id from inner_rows order by id limit 0", "NULL");
    scalar("select id from inner_rows limit 1 offset 99", "NULL");
    scalar("select payload from inner_rows order by payload desc limit 1", "30");
    scalar("select payload from inner_rows order by payload limit 1", "10");
    scalar("select id from inner_rows order by rank desc limit 1", "3");
    scalar("select id from inner_rows order by rank desc nulls last limit 1", "2");
    scalar("select id + 1 AS value from inner_rows order by value desc limit 1", "4");
    scalar("select id from inner_rows order by 1 desc limit 1", "3");
    scalar("select id from inner_rows order by 01 desc limit 1", "3");
    scalar("select id from inner_rows order by (id - 2) * (id - 2), id desc limit 1", "2");
    scalar("select 1 / (id - 2) from inner_rows order by id limit 1", "-1");
    scalar("select id from inner_rows where id = outer_rows.id order by id limit 1", "1");
    scalar("select id from inner_rows order by id offset 1 rows fetch next 1 row only", "2");
    for (bool negate : {false, true}) {
        assert((project("select 1 from inner_rows limit 0", true, negate) ==
                std::vector<std::string>{negate ? "t " : "f "}));
        assert((project("select 1 from inner_rows order by id limit 1 offset 2", true, negate) ==
                std::vector<std::string>{negate ? "f " : "t "}));
        assert((project("select 1 from inner_rows limit 1 offset 3", true, negate) ==
                std::vector<std::string>{negate ? "t " : "f "}));
    }
    bool cardinality = false;
    try {
        (void)project("select id from inner_rows order by id limit 2");
    } catch (const std::runtime_error& error) {
        cardinality = std::string(error.what()).find("SQLSTATE 21000") != std::string::npos;
    }
    assert(cardinality);
    const auto expectError = [&](const std::string& sql, const std::string& state) {
        bool rejected = false;
        std::string actualError;
        try { (void)project(sql); }
        catch (const std::runtime_error& error) {
            actualError = error.what();
            rejected = actualError.find("SQLSTATE " + state) != std::string::npos;
        }
        if (!rejected) std::cerr << sql << " expected " << state << " got " << actualError << '\n';
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    expectError("select id from inner_rows order by 1 / (id - 1) limit 1", "22012");
    expectError("select id from inner_rows order by 2 limit 0", "42P10");
    expectError("select id, payload from inner_rows limit 0", "42601");
    expectError("select id from missing_rows limit 0", "42P01");
    scalar("select id from inner_rows order by id fetch first 1 row with ties", "1");
    scalar("select id from inner_rows order by id offset 1 rows fetch next 1 row with ties", "2");
    scalar("select id from inner_rows order by rank desc fetch first 1 row with ties", "3");
    scalar("select id from inner_rows order by id fetch first 0 rows with ties", "NULL");
    expectError("select id from inner_rows fetch first 1 row with ties", "42601");
    assert(g_engine.insertRow(db, "inner_rows",
        {{"id", "4"}, {"payload", "40"}, {"rank", "10"}}) == dbms::DBStatus::OK);
    expectError("select id from inner_rows order by rank desc nulls last fetch first 1 row with ties", "21000");
    scalar("select id from inner_rows order by rank desc nulls last, id desc fetch first 1 row with ties", "4");
    scalar("select rank from inner_rows order by rank desc nulls last offset 1 rows fetch next 1 row with ties", "10");
    for (bool negate : {false, true}) {
        assert((project("select 1 from inner_rows order by rank fetch first 1 row with ties", true, negate) ==
                std::vector<std::string>{negate ? "f " : "t "}));
        assert((project("select 1 from inner_rows order by rank fetch first 0 rows with ties", true, negate) ==
                std::vector<std::string>{negate ? "t " : "f "}));
    }
    assert(g_engine.insertRow(db, "inner_rows",
        {{"id", "5"}, {"payload", "50"}, {"rank", std::nullopt}}) == dbms::DBStatus::OK);
    expectError("select id from inner_rows order by rank desc fetch first 1 row with ties", "21000");
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[PROJECTION SUBQUERY ROW SELECTION] passed\n";
}
