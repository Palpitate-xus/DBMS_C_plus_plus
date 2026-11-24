#pragma once

#include "catalog/SystemCatalogRows.h"

namespace dbms { class StorageEngine; }

namespace dbms::index_catalog {

inline constexpr Oid relationOid = 2610;
inline constexpr char name[] = "pg_index";
inline constexpr char version[] = "DBMS_INDEX_CATALOG1\n";
inline constexpr char primaryVersion[] = "DBMS_PRIMARY_INDEX_CATALOG1\n";
inline constexpr int16_t indexAttribute = 1;
inline constexpr int16_t ownerAttribute = 2;
inline constexpr std::array<system_catalog::Field, 21> fields{{
    {"indexrelid", 26, 4}, {"indrelid", 26, 4}, {"indnatts", 21, 2}, {"indnkeyatts", 21, 2},
    {"indisunique", 16, 1}, {"indnullsnotdistinct", 16, 1}, {"indisprimary", 16, 1},
    {"indisexclusion", 16, 1}, {"indimmediate", 16, 1}, {"indisclustered", 16, 1},
    {"indisvalid", 16, 1}, {"indcheckxmin", 16, 1}, {"indisready", 16, 1},
    {"indislive", 16, 1}, {"indisreplident", 16, 1}, {"indkey", 22, -1},
    {"indcollation", 30, -1}, {"indclass", 30, -1}, {"indoption", 22, -1},
    {"indexprs", 194, -1, true}, {"indpred", 194, -1, true}}};

inline const QueryRowDescriptor& descriptor() {
    static const auto columns = [] {
        auto result = system_catalog::ownedDescriptor(fields,
            {"indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique", "indnullsnotdistinct", "indisprimary",
             "indisexclusion", "indimmediate", "indisclustered", "indisvalid", "indcheckxmin", "indisready",
             "indislive", "indisreplident"});
        // pg_node_tree is materialized by the system catalog bootstrap.
        // Its declaration is not part of the bootstrap-independent types.
        QueryOutputColumn predicate{"indpred", "pg_node_tree", false, 0, fields.back().type, -1, {}};
        predicate.physicalAttributeNumber = static_cast<int16_t>(fields.size());
        result.push_back(std::move(predicate));
        for (auto& field : result) field.physicalRelationOid = relationOid;
        return result;
    }();
    return columns;
}

void synchronizePrimary(const StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, const PgClassRow& owner, const std::string& physical, bool legacy = false);

} // namespace dbms::index_catalog
