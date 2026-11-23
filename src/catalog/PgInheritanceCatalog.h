#pragma once

#include "catalog/PgDefaultCatalog.h"

namespace dbms {
class StorageEngine;
}

namespace dbms::inheritance_catalog {

inline constexpr Oid relationOid = 2611;
inline constexpr char name[] = "pg_inherits";
inline constexpr char version[] = "DBMS_INHERITANCE_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 4> fields{{
    {"inhrelid", 26, 4}, {"inhparent", 26, 4},
    {"inhseqno", 23, 4}, {"inhdetachpending", 16, 1}}};

inline const QueryRowDescriptor& descriptor() {
    static const auto columns = [] {
        auto result = system_catalog::ownedDescriptor(fields,
            {"inhrelid", "inhparent", "inhseqno", "inhdetachpending"});
        for (auto& column : result) column.physicalRelationOid = relationOid;
        return result;
    }();
    return columns;
}

std::vector<std::vector<ExprValue>> rows(StorageEngine& engine, const std::string& database);

} // namespace dbms::inheritance_catalog
