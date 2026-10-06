#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "group_collection_aggregate";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("grp", false, 4));
    table.append(dbms::makeTextColumn("value", true));
    table.append(dbms::makeTextColumn("sep", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    const std::vector<dbms::StorageEngine::SqlRow> data = {
        {{"id", "1"}, {"grp", "1"}, {"value", "b"}, {"sep", "|"}},
        {{"id", "2"}, {"grp", "1"}, {"value", std::nullopt}, {"sep", "?"}},
        {{"id", "3"}, {"grp", "1"}, {"value", ""}, {"sep", "+"}},
        {{"id", "4"}, {"grp", "1"}, {"value", "a,b"}, {"sep", ":"}},
        {{"id", "5"}, {"grp", "1"}, {"value", "NULL"}, {"sep", std::nullopt}}
    };
    for (const auto& row : data)
        assert(g_engine.insertRow(db, table.tablename, row) == dbms::DBStatus::OK);

    auto run = [&](const dbms::StorageEngine::AggItem& item, bool parallel) {
        auto scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename);
        dbms::OpPtr plan;
        if (parallel) {
            plan = std::make_unique<dbms::ParallelGroupAggregateOp>(
                std::move(scan), table, std::vector<std::string>{},
                std::vector<dbms::StorageEngine::AggItem>{item},
                std::vector<std::string>{}, 4);
        } else {
            plan = std::make_unique<dbms::GroupAggregateOp>(
                std::move(scan), table, std::vector<std::string>{},
                std::vector<std::vector<std::string>>{},
                std::vector<dbms::StorageEngine::AggItem>{item},
                std::vector<std::string>{});
        }
        return dbms::QueryPlanner::executePlanChecked(std::move(plan));
    };
    auto expect = [&](const dbms::StorageEngine::AggItem& item, const std::string& value) {
        for (bool parallel : {false, true}) {
            auto result = run(item, parallel);
            if (!result.ok) std::cerr << result.error << '\n';
            assert(result.ok);
            if (result.rows != std::vector<std::string>{value}) {
                std::cerr << item.func << '(' << item.arg << ") "
                          << (parallel ? "parallel" : "serial")
                          << " expected [" << value << "] got ["
                          << (result.rows.empty() ? "<no row>" : result.rows.front())
                          << "]\n";
            }
            assert(result.rows == std::vector<std::string>{value});
        }
    };
    expect({"string_agg", "value, sep", {}, {}}, "b+:a,bNULL");
    expect({"array_agg", "value", {}, {}}, R"({b,NULL,"","a,b","NULL"})");
    expect({"string_agg", "upper(value), ';'", {">id 2"}, "id desc"}, "NULL;A,B;");
    expect({"array_agg", "id * (id + 1)", {">id 3"}, "id desc"}, "{30,20}");
    expect({"array_agg", "DISTINCT grp", {}, {}}, "{1}");
    expect({"array_agg", "DISTINCT 5 - id", {}, {}}, "{0,1,2,3,4}");
    expect({"string_agg", "DISTINCT grp::text, '|'", {}, {}}, "1");
    expect({"string_agg", "value, sep", {">id 99"}, {}}, "NULL");
    expect({"array_agg", "value", {">id 99"}, {}}, "NULL");
    expect({"string_agg", "'', ','", {"=id 3"}, {}}, "");
    expect({"array_agg", "'q\"x'", {"=id 1"}, {}}, R"({"q\"x"})");
    expect({"array_agg", "id", {"isnullvalue"}, {}}, "{2}");
    expect({"array_agg", "id", {"isnotnullvalue"}, {}}, "{1,3,4,5}");
    for (bool parallel : {false, true}) {
        const auto failed = run({"array_agg", "1 / (id - 1)", {}, {}}, parallel);
        assert(!failed.ok);
        assert(failed.error.find("SQLSTATE 22012") != std::string::npos);
        assert(failed.errorSqlState == "22012");
        assert(failed.rows.empty() && failed.structuredRows.empty() &&
               failed.structuredNulls.empty());
    }

    // Engage real grouping workers, then compare ordered collection results.
    for (int id = 6; id <= 300; ++id)
        assert(g_engine.insert(db, table.tablename,
            {{"id", std::to_string(id)}, {"grp", "1"}, {"value", "x"}, {"sep", ","}}) ==
            dbms::DBStatus::OK);
    const dbms::StorageEngine::AggItem ordered{"array_agg", "id", {}, "id desc"};
    const auto serial = run(ordered, false);
    const auto parallel = run(ordered, true);
    assert(serial.ok && parallel.ok && serial.rows == parallel.rows);
    assert(serial.rows.front().rfind("{300,299,298,", 0) == 0);
    const dbms::StorageEngine::AggItem textOrder{"array_agg", "id::text", {}, "id::text"};
    const auto textSerial = run(textOrder, false);
    const auto textParallel = run(textOrder, true);
    assert(textSerial.ok && textParallel.ok && textSerial.rows == textParallel.rows);
    assert(textSerial.rows.front().rfind("{1,10,100,", 0) == 0);

    // Empty text is a non-NULL SQL value.  Both serial and parallel scalar
    // aggregates must count/select it instead of using string emptiness as a
    // surrogate NULL bit.
    expect({"count", "value", {}, {}}, "299");
    expect({"count", "distinct value", {}, {}}, "5");
    expect({"min", "value", {}, {}}, "");
    for (bool parallel : {false, true}) {
        const auto emptyMinimum = run({"min", "value", {}, {}}, parallel);
        assert(emptyMinimum.ok && emptyMinimum.structuredRowsAvailable);
        assert(emptyMinimum.structuredRows ==
               std::vector<std::vector<std::string>>{{""}});
        assert(emptyMinimum.structuredNulls ==
               std::vector<std::vector<bool>>{{false}});
    }

    assert(g_engine.truncateTable(db, table.tablename) == dbms::DBStatus::OK);
    expect({"array_agg", "value", {}, {}}, "NULL");
    expect({"string_agg", "value, sep", {}, {}}, "NULL");

    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[GROUP COLLECTION AGGREGATE] passed\n";
}
