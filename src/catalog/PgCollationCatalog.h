#pragma once

#include "catalog/PgDefaultCatalog.h"
#include "catalog/collation.h"

namespace dbms::collation_catalog {

inline constexpr Oid relationOid = 3456;
inline constexpr char name[] = "pg_collation";
inline constexpr char version[] = "DBMS_COLLATION_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 12> fields{{
    {"oid", 26, 4}, {"collname", 19, 64}, {"collnamespace", 26, 4}, {"collowner", 26, 4},
    {"collprovider", 18, 1}, {"collisdeterministic", 16, 1}, {"collencoding", 23, 4},
    {"collcollate", 25, -1, true}, {"collctype", 25, -1, true}, {"colllocale", 25, -1, true},
    {"collicurules", 25, -1, true}, {"collversion", 25, -1, true}}};

struct Collation {
    Oid oid;
    const char* name;
    const char* implementationName;
    char provider;
    bool deterministic;
    int32_t encoding;
    const char* libcLocale;
    const char* builtinLocale;
    const char* version;
};

// System OIDs below 9000 come from PostgreSQL's bootstrap data. The
// remaining OIDs belong to comparators supplied by this engine.
inline constexpr std::array<Collation, 7> collations{{
    {100, "default", "default", 'd', true, -1, nullptr, nullptr, nullptr},
    {950, "C", "c", 'c', true, -1, "C", nullptr, nullptr},
    {951, "POSIX", "posix", 'c', true, -1, "POSIX", nullptr, nullptr},
    {962, "ucs_basic", "ucs_basic", 'b', true, 6, nullptr, "C", "1"},
    {9002, "nocase", "nocase", 'b', false, 6, nullptr, nullptr, nullptr},
    {9003, "reverse", "reverse", 'b', true, 6, nullptr, nullptr, nullptr},
    {9004, "C.utf8", "c.utf8", 'b', true, 6, nullptr, "C.utf8", nullptr}}};

inline Oid builtinOid(const std::string& spelling) {
    if (spelling.empty()) return INVALID_OID;
    const auto resolved = collation::normalizeName(spelling);
    for (const auto& entry : collations)
        if (entry.implementationName == resolved) return entry.oid;
    return INVALID_OID;
}

inline const QueryRowDescriptor& descriptor() {
    static const auto value = [] {
        std::vector<std::string> names;
        for (const auto& field : fields) names.emplace_back(field.name);
        auto columns = system_catalog::ownedDescriptor(fields, names);
        for (auto& column : columns) column.physicalRelationOid = relationOid;
        return columns;
    }();
    return value;
}

inline std::vector<std::vector<ExprValue>> rows() {
    const auto text = [](const char* value) { return ExprValue("text", value ? value : "", !value); };
    std::vector<std::vector<ExprValue>> result;
    result.reserve(collations.size());
    for (const auto& entry : collations) {
        result.push_back({ExprValue("oid", std::to_string(entry.oid)), ExprValue("name", entry.name),
            ExprValue("oid", "11"), ExprValue("oid", "10"), ExprValue("\"char\"", std::string(1, entry.provider)),
            ExprValue("boolean", entry.deterministic ? "t" : "f"), ExprValue("integer", std::to_string(entry.encoding)),
            text(entry.libcLocale), text(entry.libcLocale), text(entry.builtinLocale), text(nullptr), text(entry.version)});
    }
    return result;
}

} // namespace dbms::collation_catalog
