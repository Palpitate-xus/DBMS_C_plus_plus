#include "access/PrimaryKeyIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "process/RuntimeStats.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("composite_primary_order");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);

    TableSchema definition;
    definition.tablename = "purchases";
    definition.formatVersion = DATA_FILE_FORMAT_VERSION;
    definition.append(makeVarCharColumn("store", false, 60));
    definition.append(makeIntColumn("ord", false, 8));
    definition.append(makeVarCharColumn("note", true, 60));
    definition.pkColIndices = {0, 1};
    assert(g_engine.createTable(database, definition) == DBStatus::OK);
    const auto name = definition.tablename;
    const std::string prefix = "same-start-for-long-store-";
    for (int order : {-2, 11, 0, -10, 3}) {
        assert(g_engine.insertRow(database, name, {{"store", prefix + "A"},
            {"ord", std::to_string(order)}, {"note", order == 0 ? std::nullopt : StorageEngine::SqlCell{"note"}}})
            == DBStatus::OK);
    }
    assert(g_engine.insertRow(database, name, {{"store", prefix + "B"},
        {"ord", "99"}, {"note", "another store"}}) == DBStatus::OK);
    const auto savedCosts = QueryPlanner::costModel();
    QueryPlanner::setCostParameter("random_page_cost", 0.01);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());

    const auto stats = [&]() {
        for (const auto& row : getRuntimeTableStats(database))
            if (row.relname == name) return row;
        return RuntimeTableStats{};
    };
    auto before = stats();
    PlanContext query;
    query.dbname = database;
    query.tablename = name;
    query.conds = StorageEngine::parseConditions({"apicond =store '" + prefix + "A'"});
    query.selectCols = {"ord", "note"};
    query.orderByCol = "ord";
    query.orderByAsc = false;
    query.offset = 1;
    query.limit = 2;
    const auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, query));
    assert(result.ok && result.structuredRows.size() == 2 && result.structuredNulls.size() == 2);
    assert(result.structuredRows[0].at(0) == "3" && result.structuredRows[0].at(1) == "note");
    assert(result.structuredRows[1].at(0) == "0" && result.structuredNulls[1].at(1));
    auto after = stats();
    assert(after.idxScan - before.idxScan == 1 && after.idxTupFetch - before.idxTupFetch == 3);

    query.orderByAsc = true;
    const auto reversed = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, query));
    assert(reversed.ok && reversed.structuredRows.size() == 2);
    assert(reversed.structuredRows[0].at(0) == "-2" && reversed.structuredRows[1].at(0) == "0");
    before = after;
    after = stats();
    assert(after.idxScan - before.idxScan == 1 && after.idxTupFetch - before.idxTupFetch == 3);

    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);

    // A structurally valid index can still point to the wrong row. Check the
    // first fetched row against its stored order key even when LIMIT is 1.
    const auto table = g_engine.getTableSchema(database, name);
    auto* index = g_engine.getPKIndex(database, name);
    assert(index && index->usesCompositePrimaryRanges());
    const auto key = [&](const std::string& order) {
        return primary_key_index::keyForValues(table, *index, {{"store", prefix + "A"}, {"ord", order}});
    };
    const auto high = key("11"), lower = key("3");
    const auto highRows = index->searchMulti(high), lowerRows = index->searchMulti(lower);
    assert(highRows.size() == 1 && lowerRows.size() == 1);
    assert(index->removeMulti(high, highRows.front()) && index->removeMulti(lower, lowerRows.front()));
    assert(index->insertMulti(high, lowerRows.front()) && index->insertMulti(lower, highRows.front()));
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    query.orderByAsc = false;
    query.offset = 0;
    query.limit = 1;
    const auto broken = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, query));
    assert(!broken.ok && broken.errorSqlState == "XX001");
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(index->removeMulti(high, lowerRows.front()) && index->removeMulti(lower, highRows.front()));
    assert(index->insertMulti(high, highRows.front()) && index->insertMulti(lower, lowerRows.front()));

    // Simulate a leading digest collision at the start of the ordered page.
    // The false candidate must not consume LIMIT or make the result shorter.
    const auto foreignKey = primary_key_index::keyForValues(table, *index,
        {{"store", prefix + "B"}, {"ord", "99"}});
    const auto foreignRows = index->searchMulti(foreignKey);
    assert(foreignRows.size() == 1);
    const auto collision = key("12");
    assert(index->insertMulti(collision, foreignRows.front()));
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    before = stats();
    const auto retried = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, query));
    assert(retried.ok && retried.structuredRows.size() == 1);
    assert(retried.structuredRows[0].at(0) == "11");
    after = stats();
    assert(after.seqScan - before.seqScan == 1 && after.idxTupFetch - before.idxTupFetch == 1);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(index->removeMulti(collision, foreignRows.front()));

    QueryPlanner::setCostModel(savedCosts);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[COMPOSITE PRIMARY ORDER] native ordered scans preserve rows, offsets and NULLs\n";
}
