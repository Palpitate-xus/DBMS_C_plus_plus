#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "table_extract_typed_dispatch", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.tablename = "dates_";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 2));
    table.append(makeDateColumn("D", false));
    table.append(makeDateColumn("day", false));
    table.append(makeIntColumn("year", false, 2));
    for (size_t column = 1; column < table.len; ++column) table.cols[column].isNull = true;
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    assert(g_engine.insertRow(db, "dates_", {{"id", "1"}, {"D", "2026-10-06"}, {"day", "2026-10-06"}, {"year", "9999"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "dates_", {{"id", "2"}, {"D", "2026-10-06"}, {"day", "2026-10-06"}, {"year", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "dates_", {{"id", "3"}, {"D", std::nullopt}, {"day", std::nullopt}, {"year", "8888"}}) == DBStatus::OK);
    const auto expression = [](std::vector<std::string> arguments) {
        StorageEngine::SelectExpr result;
        result.isScalar = true;
        result.funcName = "extract";
        result.funcArgs = std::move(arguments);
        return result;
    };
    for (const auto& arguments : std::vector<std::vector<std::string>>{
             {"year", "day"}, {"year", "\"D\""}, {"\"YEAR\"", "\"D\""},
             {"'year'", "\"D\""}, {"EXTRACT(year FrOm \"D\")"}}) {
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        g_engine.queryExpr(db, "dates_", {}, {expression(arguments)}, {}, &rows, &nulls);
        if (rows.size() != 3 || rows[0][0] != "2026" || rows[1][0] != "2026" ||
            nulls[0][0] || nulls[1][0] || !nulls[2][0]) {
            std::cerr << "EXTRACT legacy field=" << arguments.front() << " value="
                      << (rows.empty() ? "no row" : rows.front().front()) << '\n';
            assert(false);
        }
    }
    const auto error = [&](std::vector<std::string> arguments, const std::string& state) {
        bool rejected = false;
        try { g_engine.queryExpr(db, "dates_", {"=id 1"}, {expression(std::move(arguments))}); }
        catch (const DbError& failure) { rejected = failure.sqlState() == state; }
        catch (const std::exception& failure) {
            rejected = std::string(failure.what()).find("SQLSTATE " + state) != std::string::npos;
        }
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    error({"NoSuchUnit", "\"D\""}, "22023");
    error({"hour", "\"D\""}, "0A000");
    error({"year", "missing"}, "42703");
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[TABLE EXTRACT TYPED DISPATCH] field roles and nullable operands passed\n";
}
