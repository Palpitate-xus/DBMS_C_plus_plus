#pragma once

#include "catalog/SystemCatalogRows.h"
#include "catalog/TypeCatalogScan.h"

namespace dbms::description_catalog {

inline constexpr char name[] = "pg_description";
inline constexpr Oid relationOid = 2609;
inline constexpr char versionFile[] = ".description_catalog_version";
inline constexpr char version[] = "DBMS_DESCRIPTION_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 4> fields{{
    {"objoid", 26, 4}, {"classoid", 26, 4}, {"objsubid", 23, 4}, {"description", 25, -1}
}};

inline const QueryRowDescriptor& descriptor() {
    static const auto columns = [] {
        auto result = system_catalog::ownedDescriptor(fields, {"objoid", "classoid", "objsubid", "description"});
        for (auto& column : result) column.physicalRelationOid = relationOid;
        return result;
    }();
    return columns;
}

bool needsStagedUpgrade(const std::filesystem::path& catalogDirectory);
bool upgradeStagedDeclaration(const std::filesystem::path& catalogDirectory);

inline std::optional<Oid> objectFilter(const PreparedQuery::SourceRange& range) {
    return catalogOidLiteralValue(catalogOidEqualityInput(range, true, 1, true), true);
}

inline std::vector<ExprValue> cells(const PgDescriptionRow& row) {
    return {ExprValue("oid", std::to_string(row.objoid)), ExprValue("oid", std::to_string(row.classoid)),
            ExprValue("integer", std::to_string(row.objsubid)), ExprValue("text", row.description)};
}

} // namespace dbms::description_catalog
