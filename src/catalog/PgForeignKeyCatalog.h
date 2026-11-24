#pragma once

#include "catalog/PgDefaultCatalog.h"

namespace dbms {
class StorageEngine;
}

namespace dbms::foreign_key_catalog {

inline constexpr Oid relationOid = 2606;
inline constexpr char name[] = "pg_constraint";
inline constexpr char version[] = "DBMS_FOREIGN_KEY_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 28> fields{{
    {"oid", 26, 4}, {"conname", 19, 64}, {"connamespace", 26, 4}, {"contype", 18, 1},
    {"condeferrable", 16, 1}, {"condeferred", 16, 1}, {"conenforced", 16, 1}, {"convalidated", 16, 1},
    {"conrelid", 26, 4}, {"contypid", 26, 4}, {"conindid", 26, 4}, {"conparentid", 26, 4},
    {"confrelid", 26, 4}, {"confupdtype", 18, 1}, {"confdeltype", 18, 1}, {"confmatchtype", 18, 1},
    {"conislocal", 16, 1}, {"coninhcount", 21, 2}, {"connoinherit", 16, 1}, {"conperiod", 16, 1},
    {"conkey", 1005, -1, true}, {"confkey", 1005, -1, true}, {"conpfeqop", 1028, -1, true},
    {"conppeqop", 1028, -1, true}, {"conffeqop", 1028, -1, true}, {"confdelsetcols", 1005, -1, true},
    {"conexclop", 1028, -1, true}, {"conbin", 194, -1, true}}};

inline const QueryRowDescriptor& descriptor() {
    static const auto columns = [] {
        auto result = system_catalog::ownedDescriptor(fields,
            {"oid", "conname", "connamespace", "contype", "condeferrable", "condeferred", "conenforced",
             "convalidated", "conrelid", "contypid", "conparentid", "confrelid", "conislocal",
             "coninhcount", "conperiod", "conkey", "confkey"});
        for (auto& column : result) {
            column.physicalRelationOid = relationOid;
            if (column.typeOid == 1005) column.type = "smallint[]";
        }
        return result;
    }();
    return columns;
}

void synchronize(const StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, const PgClassRow& owner, const std::string& physical,
    const std::optional<std::pair<std::string, std::string>>& rename = {});
std::vector<std::vector<ExprValue>> rows(const StorageEngine& engine, const std::string& database,
    std::optional<Oid> owner = {}, std::optional<Oid> referenced = {});

}  // namespace dbms::foreign_key_catalog
