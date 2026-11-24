#pragma once

#include "catalog/SystemCatalogRows.h"

namespace dbms {
class StorageEngine;
}

namespace dbms::roles_catalog {

inline constexpr char name[] = "pg_roles";
inline constexpr std::array<system_catalog::Field, 13> fields{{
    {"rolname", 19, 64, true}, {"rolsuper", 16, 1, true}, {"rolinherit", 16, 1, true},
    {"rolcreaterole", 16, 1, true}, {"rolcreatedb", 16, 1, true}, {"rolcanlogin", 16, 1, true},
    {"rolreplication", 16, 1, true}, {"rolconnlimit", 23, 4, true}, {"rolpassword", 25, -1, true},
    {"rolvaliduntil", 1184, 8, true}, {"rolbypassrls", 16, 1, true}, {"rolconfig", 1009, -1, true},
    {"oid", 26, 4, true}}};
inline constexpr Oid oidAttributeNumber = 13;

bool bootstrap(CatalogManager& catalog);
QueryRowDescriptor descriptor(const CatalogManager::MetadataSnapshot& catalog);
std::vector<std::vector<ExprValue>> rows(const StorageEngine& engine, bool readExpiry,
    std::optional<Oid> roleOid = {});

} // namespace dbms::roles_catalog
