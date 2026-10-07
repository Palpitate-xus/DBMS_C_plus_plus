#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <fstream>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("native_legacy_array_schema");
    {
        StorageEngine owner;
        assert(owner.createDatabase(db, "utf8") == DBStatus::OK);
        TableSchema schema; schema.len = 1; schema.cols[0].dataName = "items";
        assert(TypeRegistry::instance().resolveColumnType(schema.cols[0], "integer", {}, true).empty());
        schema.cols[0].isNull = true;
        assert(owner.createTable(db, "old_source", schema) == DBStatus::OK);
        assert(owner.getTableSchema(db, "old_source").physicalRelationId == 1);
    }
    // Actual schema bytes produced by the unchanged 6aec public resolve/create
    // API, SHA256 4a6cafda8173e17f943607c1e643a9287c5ccceb38b603b79ee356fc0cdfa6c6.
    // Install only in this new isolated table, preserving its real RID1. No
    // metadata layout/SID/checksum is invented and no production file is edited.
    const std::string hex =
        "0a0042440100000005696e74656765725b5d000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000006974656d73000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000004000000000001000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000"
        "000000000000020000000a0070675f64656661756c740000000000004348"
        "4b320000000049444e31010000444654310000524c423100000000524944"
        "310100000000000000";
    std::string bytes;
    for (size_t position = 0; position < hex.size(); position += 2)
        bytes.push_back(static_cast<char>(std::stoul(hex.substr(position, 2), nullptr, 16)));
    {
        std::ofstream fixture(db + "/old_source.stc", std::ios::binary | std::ios::trunc);
        assert(fixture); fixture.write(bytes.data(), bytes.size()); assert(fixture);
    }
    {
        StorageEngine cold;
        const auto stored = cold.getTableSchema(db, "old_source");
        assert(stored.physicalRelationId == 1 && stored.cols[0].dataType == "integer[]" &&
               stored.cols[0].isArray && stored.cols[0].isVariableLength && stored.cols[0].dsize == 4);
        const auto inserted = cold.insertRow(db, "old_source", {{"items", "{1,2}"}});
        std::cout << "NATIVE_LEGACY_ARRAY_EXACT_SEED_STATUS=" << int(inserted) << std::endl;
        assert(inserted == DBStatus::OK && !cold.inTransaction());
        assert(cold.insertRow(db, "old_source", {{"items", "[0:1]={3,NULL}"}}) == DBStatus::OK);
        assert(cold.insertRow(db, "old_source", {{"items", "{bad}"}}) == DBStatus::INVALID_VALUE);
        assert(!cold.inTransaction());
        auto prepared = cold.prepareBoundQuery(db, "SELECT items FROM old_source");
        auto plan = QueryPlanner::buildPreparedSelectPlan(&cold, db, "old_source", std::move(prepared));
        auto result = QueryPlanner::executePlanChecked(std::move(plan)); result.throwIfFailed();
        assert(result.structuredRowsAvailable && result.structuredRows == std::vector<std::vector<std::string>>({{"{1,2}"},{"[0:1]={3,NULL}"}}));
        assert(result.structuredNulls == std::vector<std::vector<bool>>({{false},{false}}));
        assert(cold.getTableSchema(db, "old_source").physicalRelationId == 1);
    }
    std::cout << "[NATIVE LEGACY ARRAY SCHEMA] real old schema/cold/input/NULL/rollback/RID passed\n";
}
