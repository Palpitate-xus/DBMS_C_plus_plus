#pragma once
#include "catalog/systables.h"
#include "parser/query_binding.h"

namespace dbms {
// Only fields actually owned by PgNamespaceRow. nspacl is not stored;
// binding must reject a complete-row wildcard or an ACL field request.
inline const QueryRowDescriptor& ownedNamespaceCatalogDescriptor() {
    static const QueryRowDescriptor columns={
        {"oid","oid"},{"nspname","name"},{"nspowner","oid"}
    };
    return columns;
}

inline std::vector<ExprValue> ownedNamespaceCatalogCells(const PgNamespaceRow& row) {
    return {ExprValue("oid",std::to_string(row.oid),false),
            ExprValue("name",row.nspname,false),
            ExprValue("oid",std::to_string(row.nspowner),false)};
}
} // namespace dbms
