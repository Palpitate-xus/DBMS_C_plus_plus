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
    const auto database = testDbPath("composite_primary_fingerprint");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema definition;
    definition.tablename = "orders";
    definition.formatVersion = DATA_FILE_FORMAT_VERSION;
    definition.append(makeVarCharColumn("shop", false, 60));
    definition.append(makeVarCharColumn("number", false, 60));
    definition.append(makeVarCharColumn("note", false, 60));
    definition.pkColIndices = {0, 1};
    assert(g_engine.createTable(database, definition) == DBStatus::OK);
    const auto schema = g_engine.getTableSchema(database, definition.tablename);
    const std::map<std::string, std::string> first{{"shop", "a"},
        {"number", "b\x01" "c"}, {"note", "first"}};
    const std::map<std::string, std::string> second{{"shop", "a\x01" "b"},
        {"number", "c"}, {"note", "second"}};
    const std::map<std::string, std::string> empty{{"shop", ""}, {"number", ""}, {"note", "empty"}};
    assert(schema.buildPKValue(first) == schema.buildPKValue(second));
    for (const auto* row : {&first, &second, &empty})
        assert(g_engine.insert(database, schema.tablename, *row) == DBStatus::OK);

    auto* index = g_engine.getPKIndex(database, schema.tablename);
    assert(index && index->usesCompositePrimaryFingerprints() &&
           index->hasCompletePrimaryKeys());
    const auto firstKey = primary_key_index::keyForValues(schema, *index, first);
    const auto secondKey = primary_key_index::keyForValues(schema, *index, second);
    const auto emptyKey = primary_key_index::keyForValues(schema, *index, empty);
    const auto hex = [](const std::string& bytes) {
        std::string output;
        constexpr char digits[] = "0123456789abcdef";
        for (unsigned char byte : bytes) {
            output.push_back(digits[byte >> 4]);
            output.push_back(digits[byte & 15]);
        }
        return output;
    };
    // Known SHA-256 vectors over two independently length-framed fields.
    assert(hex(firstKey) == "35a50c10cdebb74730c245bd63e0af54d3b9dd85");
    assert(hex(secondKey) == "fd6743160daff144afdeb8de786345e6bd2dc043");
    assert(hex(emptyKey) == "374708fff7719dd5979ec875d56cd2286f6d3cf7");
    assert(!index->insertMulti("incorrect-width", 9999));
    assert(index->searchMulti(firstKey).size() == 1);
    const auto secondPositions = index->searchMulti(secondKey);
    assert(secondPositions.size() == 1);
    assert(index->searchMulti(emptyKey).size() == 1);

    // Put a distinct row in the same physical bucket to exercise the full
    // SQL recheck independently of an actual SHA-256 collision.
    assert(index->insertMulti(firstKey, secondPositions.front()));
    const auto stats = [&]() {
        for (const auto& row : getRuntimeTableStats(database))
            if (row.relname == schema.tablename) return row;
        return RuntimeTableStats{};
    };
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    const auto before = stats();
    PlanContext context;
    context.dbname = database;
    context.tablename = schema.tablename;
    context.selectCols = {"note"};
    context.conds = StorageEngine::parseConditions({"apicond =shop 'a'", "apicond =number 'b\x01" "c'"});
    const auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine, context));
    assert(result.ok && result.structuredRows == std::vector<std::vector<std::string>>{{"first"}});
    const auto after = stats();
    assert(after.idxScan - before.idxScan == 1 && after.idxTupFetch - before.idxTupFetch == 2);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.insert(database, schema.tablename, first) == DBStatus::DUPLICATE_KEY);
    assert(index->removeMulti(firstKey, secondPositions.front()));
    assert(index->searchMulti(firstKey).size() == 1);
    assert(index->searchMulti(secondKey) == secondPositions);

    const std::map<std::string, std::string> other{{"shop", "a"},
        {"number", "new"}, {"note", "other"}};
    const auto otherKey = primary_key_index::keyForValues(schema, *index, other);
    assert(index->insertMulti(otherKey, secondPositions.front()));
    assert(g_engine.insert(database, schema.tablename, other) == DBStatus::OK);
    assert(index->searchMulti(otherKey).size() == 2);
    assert(index->removeMulti(otherKey, secondPositions.front()));
    assert(index->searchMulti(otherKey).size() == 1);
    assert(index->flush());
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[COMPOSITE PRIMARY FINGERPRINT] stable encoding and full-value collision recheck passed\n";
}
