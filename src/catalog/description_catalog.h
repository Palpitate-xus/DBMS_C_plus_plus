#pragma once

#include "catalog/TypeCatalogScan.h"

namespace dbms::description_catalog {

inline constexpr char name[] = "pg_description";

inline QueryRowDescriptor descriptor() {
    return {{"objoid", "oid", false, 0, 26, -1, {}, 1},
            {"classoid", "oid", false, 0, 26},
            {"objsubid", "integer", false, 0, 23},
            {"description", "text", false, 0, 25}};
}

inline std::optional<Oid> objectFilter(const PreparedQuery::SourceRange& range) {
    return catalogOidLiteralValue(catalogOidEqualityInput(range, true, 1, true), true);
}

inline std::vector<ExprValue> cells(const PgDescriptionRow& row) {
    return {ExprValue("oid", std::to_string(row.objoid)), ExprValue("oid", std::to_string(row.classoid)),
            ExprValue("integer", std::to_string(row.objsubid)), ExprValue("text", row.description)};
}

} // namespace dbms::description_catalog
