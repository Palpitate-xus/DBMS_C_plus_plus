#pragma once

#include "catalog/PgDefaultCatalog.h"

namespace dbms::access_method_catalog {

inline constexpr Oid relationOid = 2601;
inline constexpr char name[] = "pg_am";
inline constexpr Oid heapOid = 2;
inline constexpr Oid btreeOid = 403;
inline constexpr char version[] = "DBMS_ACCESS_METHOD_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 4> fields{{
    {"oid", 26, 4}, {"amname", 19, 64}, {"amhandler", 24, 4}, {"amtype", 18, 1}}};

struct Method {
    Oid oid;
    std::string_view name;
    char kind;
};

// Heap and the six PostgreSQL methods use their standard catalog identities.
// The two local methods use IDs in the project's reserved system range.
inline constexpr std::array<Method, 9> methods{{
    {heapOid, "heap", 't'}, {btreeOid, "btree", 'i'}, {405, "hash", 'i'},
    {783, "gist", 'i'}, {2742, "gin", 'i'}, {3580, "brin", 'i'},
    {4000, "spgist", 'i'}, {9000, "bloom", 'i'}, {9001, "fulltext", 'i'}}};

inline Oid indexOid(std::string_view physicalMethod) {
    if (physicalMethod == "composite") return btreeOid;
    for (const auto& method : methods)
        if (method.kind == 'i' && method.name == physicalMethod) return method.oid;
    return INVALID_OID;
}

inline const QueryRowDescriptor& descriptor() {
    static const auto result = [] {
        auto columns = system_catalog::ownedDescriptor(fields, {"oid", "amname", "amtype"});
        for (auto& column : columns) column.physicalRelationOid = relationOid;
        return columns;
    }();
    return result;
}

inline std::vector<std::vector<ExprValue>> rows() {
    std::vector<std::vector<ExprValue>> result;
    result.reserve(methods.size());
    for (const auto& method : methods) {
        result.push_back({ExprValue("oid", std::to_string(method.oid)),
            ExprValue("name", std::string(method.name)),
            ExprValue("\"char\"", std::string(1, method.kind))});
    }
    return result;
}

} // namespace dbms::access_method_catalog
