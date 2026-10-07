#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <map>
#include <optional>

namespace {
void expectRows(dbms::StorageEngine& owner, const std::string& db, const std::string& table,
                const std::vector<std::pair<std::string, bool>>& expected) {
    const auto schema = owner.getTableSchema(db, table);
    std::vector<std::pair<std::string, bool>> actual;
    assert(owner.forEachRow(db, table, [&](uint32_t page, uint16_t slot, const char* data, size_t size) {
        const auto rid = dbms::StorageEngine::encodeRid(page, slot);
        const bool isNull = owner.isColumnNullByRid(db, table, rid, 0);
        const auto value = owner.extractColumnValue(std::string(data, size), schema, 0, db);
        actual.emplace_back(value, isNull);
    }));
    auto orderedExpected = expected;
    std::sort(actual.begin(), actual.end()); std::sort(orderedExpected.begin(), orderedExpected.end());
    if (actual != orderedExpected) {
        for (const auto& row : actual) std::cerr << "ARRAY_NATIVE_ACTUAL value=" << row.first << " null=" << row.second << '\n';
    }
    assert(actual == orderedExpected);
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("native_resolved_array_storage");
    {
        StorageEngine owner;
        assert(owner.createDatabase(db, "utf8") == DBStatus::OK);
        // These are the exact original public helper/create/insert inputs.
        // resolveColumnType's public full type spelling must remain unchanged.
        TableSchema arraySchema; arraySchema.len = 1;
        arraySchema.cols[0].dataName = "items";
        assert(TypeRegistry::instance().resolveColumnType(arraySchema.cols[0], "integer", {}, true).empty());
        assert(arraySchema.cols[0].dataType == "integer[]" && arraySchema.cols[0].isArray &&
               arraySchema.cols[0].isVariableLength && arraySchema.cols[0].dsize == 4);
        assert(owner.createTable(db, "array_source", arraySchema) == DBStatus::OK);
        const auto first = owner.insertRow(db, "array_source", {{"items", "{1,2}"}});
        std::cout << "NATIVE_ARRAY_EXACT_SEED_STATUS=" << int(first) << std::endl;
        assert(first == DBStatus::OK);
        assert(arraySchema.cols[0].dataType == "integer[]"); // const caller metadata is not mutated.
        const auto stored = owner.getTableSchema(db, "array_source");
        assert(stored.cols[0].dataType == "integer" && stored.cols[0].isArray &&
               stored.cols[0].isVariableLength && stored.cols[0].dsize == 4);
        assert(stored.cols[0].defaultValue.empty() && stored.cols[0].domainName.empty() &&
               stored.cols[0].defaultOrigin == arraySchema.cols[0].defaultOrigin);
        expectRows(owner, db, "array_source", {{"{1,2}", false}});
        assert(owner.createIndex(db, "array_source", "items", true, {}, "", "", false, true) == DBStatus::OK);
        assert(owner.insertRow(db, "array_source", {{"items", "{01,02}"}}) == DBStatus::DUPLICATE_KEY);
        assert(!owner.inTransaction());
        expectRows(owner, db, "array_source", {{"{1,2}", false}});
        assert(!owner.inTransaction());
        assert(owner.insertRow(db, "array_source", {{"items", "[0:1]={3,NULL}"}}) == DBStatus::OK);
        assert(owner.insertRow(db, "array_source", {{"items", "[0:1][3:4]={{4,5},{6,NULL}}"}}) == DBStatus::OK);
        assert(owner.insertRow(db, "array_source", {{"items", "{}"}}) == DBStatus::OK);
        const std::vector<std::pair<std::string, bool>> positive = {
            {"{1,2}", false}, {"[0:1]={3,NULL}", false}, {"[0:1][3:4]={{4,5},{6,NULL}}", false}, {"{}", false}};
        expectRows(owner, db, "array_source", positive);
        for (const auto& bad : {"{2147483648}", "{bad}", "[0:2]={1,2}", "{{1,2},{3}}"}) {
            assert(owner.insertRow(db, "array_source", {{"items", bad}}) == DBStatus::INVALID_VALUE);
            assert(!owner.inTransaction()); expectRows(owner, db, "array_source", positive);
        }
        assert(owner.insertRow(db, "array_source", std::map<std::string, std::optional<std::string>>{{"items", std::nullopt}}) == DBStatus::NULL_NOT_ALLOWED);
        expectRows(owner, db, "array_source", positive);
        assert(owner.beginTransaction(db) == DBStatus::OK);
        assert(owner.insertRow(db, "array_source", {{"items", "{7,NULL,8}"}}) == DBStatus::OK);
        assert(owner.insertRow(db, "array_source", {{"items", "{bad}"}}) == DBStatus::INVALID_VALUE);
        assert(owner.inTransaction());
        auto parentRows = positive; parentRows.push_back({"{7,NULL,8}", false});
        expectRows(owner, db, "array_source", parentRows);
        assert(owner.rollbackTransaction() == DBStatus::OK);
        expectRows(owner, db, "array_source", positive);
        // The factory's already element-shaped representation is unchanged.
        TableSchema factory; factory.len = 1; factory.cols[0] = makeIntColumn("items", false, 4);
        factory.cols[0].isArray = true; factory.cols[0].isVariableLength = true;
        assert(owner.createTable(db, "factory_source", factory) == DBStatus::OK);
        assert(owner.insertRow(db, "factory_source", {{"items", "{1,2}"}}) == DBStatus::OK);
        expectRows(owner, db, "factory_source", {{"{1,2}", false}});
        TableSchema nullable = arraySchema; nullable.cols[0].isNull = true;
        assert(owner.createTable(db, "nullable_source", nullable) == DBStatus::OK);
        assert(owner.insertRow(db, "nullable_source", std::map<std::string, std::optional<std::string>>{{"items", std::nullopt}}) == DBStatus::OK);
        assert(owner.insertRow(db, "nullable_source", {{"items", "{NULL}"}}) == DBStatus::OK);
        assert(owner.insertRow(db, "nullable_source", {{"items", "{}"}}) == DBStatus::OK);
        expectRows(owner, db, "nullable_source", {{"", true}, {"{NULL}", false}, {"{}", false}});
        // Primitive aliases/element widths remain real scalar element metadata.
        for (const auto& pair : std::vector<std::pair<std::string, std::string>>{{"smallint", "{32767,NULL}"}, {"bigint", "{9223372036854775807,NULL}"}, {"text", "{\"NULL\",NULL,\"\",\"é😀\"}"}}) {
            TableSchema metadata; metadata.len = 1; metadata.cols[0].dataName = "items";
            assert(TypeRegistry::instance().resolveColumnType(metadata.cols[0], pair.first, {}, true).empty());
            const auto table = "source_" + pair.first;
            assert(owner.createTable(db, table, metadata) == DBStatus::OK);
            assert(owner.insertRow(db, table, {{"items", pair.second}}) == DBStatus::OK);
            const auto canonical = pair.first == "text" ? "{\"NULL\",NULL,\"\",é😀}" : pair.second;
            expectRows(owner, db, table, {{canonical, false}});
        }
    }
    {
        StorageEngine reopened;
        const auto stored = reopened.getTableSchema(db, "array_source");
        assert(stored.cols[0].dataType == "integer" && stored.cols[0].isArray && stored.cols[0].dsize == 4);
        const auto indexes = reopened.getIndexMetadata(db, "array_source");
        assert(indexes.size() == 1 && indexes[0].name == "items" && indexes[0].isUnique);
        expectRows(reopened, db, "array_source", {{"{1,2}", false}, {"[0:1]={3,NULL}", false}, {"[0:1][3:4]={{4,5},{6,NULL}}", false}, {"{}", false}});
        auto prepared = reopened.prepareBoundQuery(db, "SELECT items,array_dims(items),array_ndims(items) FROM array_source");
        auto tree = QueryPlanner::buildPreparedSelectPlan(&reopened, db, "array_source", std::move(prepared));
        const auto result = QueryPlanner::executePlanChecked(std::move(tree)); result.throwIfFailed();
        assert(result.structuredRowsAvailable && result.structuredRows.size() == 4 && result.structuredNulls.size() == 4);
        std::vector<std::pair<std::vector<std::string>, std::vector<bool>>> actual;
        for (size_t row = 0; row < result.structuredRows.size(); ++row) actual.emplace_back(result.structuredRows[row], result.structuredNulls[row]);
        std::vector<std::pair<std::vector<std::string>, std::vector<bool>>> expected = {
            {{"{1,2}", "[1:2]", "1"}, {false,false,false}},
            {{"[0:1]={3,NULL}", "[0:1]", "1"}, {false,false,false}},
            {{"[0:1][3:4]={{4,5},{6,NULL}}", "[0:1][3:4]", "2"}, {false,false,false}},
            {{"{}", "", ""}, {false,true,true}}};
        std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
        assert(actual == expected && !reopened.inTransaction());
    }
    std::cout << "[NATIVE RESOLVED ARRAY STORAGE] exact original API/values/bounds/NULL/overflow/parent/cold/type passed\n";
}
