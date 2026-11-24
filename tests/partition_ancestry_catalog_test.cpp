#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/QueryHostProvider.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;

using namespace dbms;

namespace {

void require(bool passes, const char* description) {
    if (!passes) throw std::runtime_error(description);
}

Oid createRelation(CatalogManager& catalog, const std::string& database, Oid space,
                   const std::string& name, const std::string& member = {}) {
    TableSchema schema;
    schema.tablename = name;
    schema.append(makeIntColumn("id", true, 2));
    if (!member.empty()) {
        schema.partitionType = TableSchema::PartitionType::Range;
        schema.partitionKey = "id";
    }
    require(g_engine.createTable(database, schema) == DBStatus::OK, "create a stored table");
    if (!member.empty())
        require(g_engine.attachPartition(database, name, member, "FOR VALUES FROM (0) TO (100)") == DBStatus::OK,
                "create a parent-owned partition fork");
    PgClassRow cls;
    cls.relname = name;
    cls.relnamespace = space;
    cls.relkind = 'r';
    cls.relnatts = 1;
    cls.relam = member.empty() ? 2 : 0;
    const auto oid = catalog.createClass(cls);
    require(oid != INVALID_OID, "register a table identity");
    return oid;
}

void flagLegacyPartition(CatalogManager& catalog, Oid oid) {
    auto cls = catalog.getClass(oid);
    require(cls.has_value(), "find the legacy table row");
    cls->relispartition = true;
    require(catalog.updateClass(oid, *cls) && catalog.persistAll(), "persist the legacy partition flag");
}

std::vector<Oid> ancestry(const std::string& database, Oid oid) {
    std::vector<Oid> result;
    for (const auto& row : readPartitionAncestors(g_engine, database, nullptr, ExprValue("oid", std::to_string(oid)))) {
        require(row.typeName == "regclass" && !row.isNull && row.objectOid, "ancestry retains the relation identity");
        result.push_back(*row.objectOid);
    }
    return result;
}

void mustReject(const std::string& database, Oid oid) {
    try {
        (void)ancestry(database, oid);
    } catch (const DbError& error) {
        require(error.sqlState() == "XX001", "inconsistent ancestry reports a catalog error");
        return;
    }
    throw std::runtime_error("accepted an unprovable partition relationship");
}

} // namespace

int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto database = testDbPath("partition_ancestry_catalog");
        cleanupTestDb("partition_ancestry_catalog");
        require(g_engine.createDatabase(database) == DBStatus::OK, "create test database");
        auto catalog = g_engine.catalogService().acquire(database);
        const auto space = catalog->getNamespaceByName("public");
        require(space.has_value(), "find public schema");
        const auto root = createRelation(*catalog, database, space->oid, "root", "mid");
        const auto mid = createRelation(*catalog, database, space->oid, "mid", "leaf");
        const auto other = createRelation(*catalog, database, space->oid, "other", "leaf");
        const auto leaf = createRelation(*catalog, database, space->oid, "leaf");
        require(catalog->persistAll(), "persist the fixture before checking an ordinary table");
        require(ancestry(database, leaf).empty(), "matching a fork name alone does not make a partition");

        flagLegacyPartition(*catalog, leaf);
        flagLegacyPartition(*catalog, mid);
        mustReject(database, leaf);
        require(g_engine.detachPartition(database, "other", "leaf") == DBStatus::OK, "remove the second matching bound");
        require(ancestry(database, leaf) == std::vector<Oid>{leaf, mid, root}, "return the stored chain leaf first");

        const auto orphan = createRelation(*catalog, database, space->oid, "orphan");
        flagLegacyPartition(*catalog, orphan);
        mustReject(database, orphan);
        g_engine.catalogService().evict(database);
        require(ancestry(database, leaf) == std::vector<Oid>{leaf, mid, root}, "read the same stored chain after eviction");
        require(!g_engine.catalogService().has(database), "the cold read does not populate the mutable catalog");
        std::cout << "PARTITION_ANCESTRY_CATALOG_PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
