#pragma once
#include "catalog/SystemCatalogDeclaration.h"
#include "catalog/SystemCatalogRows.h"
#include <filesystem>
#include <fstream>

namespace dbms::default_catalog {
inline constexpr Oid relationOid = 2604;
inline constexpr Oid dependencyOid = 2608;
inline constexpr char version[] = "DBMS_ATTRIBUTE_DEFAULT_CATALOG1\n";
inline constexpr std::array<system_catalog::Field, 4> fields{{
    {"oid", 26, 4}, {"adrelid", 26, 4}, {"adnum", 21, 2}, {"adbin", 194, -1}}};
inline constexpr std::array<system_catalog::Field, 7> dependencyFields{{
    {"classid", 26, 4}, {"objid", 26, 4}, {"objsubid", 23, 4},
    {"refclassid", 26, 4}, {"refobjid", 26, 4}, {"refobjsubid", 23, 4}, {"deptype", 18, 1}}};
inline bool published(const std::filesystem::path& directory) {
    std::error_code error;
    const auto path = directory / ".attribute_default_catalog_version";
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory || (!error && !std::filesystem::exists(status))) return false;
    if (error) throw DbError("58030", "cannot inspect attribute default catalog version");
    if (!std::filesystem::is_regular_file(status)) throw DbError("XX001", "invalid attribute default catalog version");
    std::ifstream input(path, std::ios::binary);
    if (!input) throw DbError("58030", "cannot read attribute default catalog version");
    const std::string bytes((std::istreambuf_iterator<char>(input)), {});
    if (input.bad() || bytes != version) throw DbError("XX001", "invalid attribute default catalog version");
    return true;
}
template<size_t N> inline bool bootstrapOne(CatalogManager& catalog, Oid oid, const char* name,
    const std::array<system_catalog::Field, N>& declaration, bool requireExisting) {
    const auto relation = catalog.getClass(oid);
    Oid rowOid = INVALID_OID, arrayOid = INVALID_OID;
    if (relation) {
        rowOid = relation->reltype;
        const auto type = catalog.getType(rowOid);
        if (!type || !type->typarray) throw DbError("XX001", "missing actual attribute default catalog row type");
        arrayOid = type->typarray;
    } else {
        if (requireExisting) throw DbError("XX001", "missing published attribute default catalog declaration");
        if (catalog.getClassByName(name, 11) || catalog.getTypeByName(name, 11) || catalog.getTypeByName("_" + std::string(name), 11))
            throw DbError("XX001", "orphan attribute default catalog declaration");
        rowOid = catalog.allocateOid(); arrayOid = catalog.allocateOid();
    }
    return system_catalog::bootstrap(catalog, oid, name, rowOid, arrayOid, declaration, requireExisting);
}
inline bool bootstrap(CatalogManager& catalog, bool requireExisting) {
    const bool defaults = bootstrapOne(catalog, relationOid, "pg_attrdef", fields, requireExisting);
    const bool dependencies = bootstrapOne(catalog, dependencyOid, "pg_depend", dependencyFields, requireExisting);
    return defaults || dependencies;
}
template<size_t N> inline void validate(const CatalogManager::MetadataSnapshot& snapshot, Oid oid,
    const char* name, const std::array<system_catalog::Field, N>& declaration) {
    const auto relation = std::find_if(snapshot.relations.begin(), snapshot.relations.end(), [&](const auto& row) { return row.oid == oid; });
    const auto type = relation == snapshot.relations.end() ? snapshot.types.end() :
        std::find_if(snapshot.types.begin(), snapshot.types.end(), [&](const auto& row) { return row.oid == relation->reltype; });
    if (type == snapshot.types.end()) throw DbError("XX001", "missing actual attribute default catalog declaration");
    system_catalog::validateRead(snapshot, oid, name, type->oid, type->typarray, declaration, N);
}
inline const QueryRowDescriptor& descriptor() {
    // The full declaration owns all four fields. General PostgreSQL node
    // serialization is not complete yet; expose no made-up adbin value.
    static const auto value = system_catalog::ownedDescriptor(fields, {"oid", "adrelid", "adnum"});
    return value;
}
inline const QueryRowDescriptor& dependencyDescriptor() {
    static const auto value = system_catalog::ownedDescriptor(dependencyFields,
        {"classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid", "deptype"});
    return value;
}
inline std::vector<std::vector<ExprValue>> rows(const CatalogManager::MetadataSnapshot& snapshot, bool dependencies) {
    if (!snapshot.defaultObjectsIncluded)
        throw DbError("XX001", "attribute default catalog source requires its actual object snapshot");
    std::vector<std::vector<ExprValue>> result;
    if (dependencies) {
        validate(snapshot, dependencyOid, "pg_depend", dependencyFields);
        result.reserve(snapshot.dependencies.size());
        for (const auto& d : snapshot.dependencies) result.push_back({ExprValue("oid", std::to_string(d.classid)),
            ExprValue("oid", std::to_string(d.objid)), ExprValue("integer", std::to_string(d.objsubid)),
            ExprValue("oid", std::to_string(d.refclassid)), ExprValue("oid", std::to_string(d.refobjid)),
            ExprValue("integer", std::to_string(d.refobjsubid)), ExprValue("\"char\"", std::string(1, d.deptype))});
    } else {
        validate(snapshot, relationOid, "pg_attrdef", fields);
        result.reserve(snapshot.defaults.size());
        for (const auto& d : snapshot.defaults) result.push_back({ExprValue("oid", std::to_string(d.oid)),
            ExprValue("oid", std::to_string(d.adrelid)), ExprValue("smallint", std::to_string(d.adnum))});
    }
    return result;
}
} // namespace dbms::default_catalog
