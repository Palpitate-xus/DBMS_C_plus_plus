#pragma once

#include "dbms_defs.h"
#include <string>

namespace dbms {
class CatalogManager;
class StorageEngine;

namespace index_catalog {

void registerExplicitAttributes(const StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, Oid index);
void rebuildExplicitColumnIndexes(const StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, Oid owner, const std::string& column);
bool dropExplicitColumnIndexes(StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, Oid owner, const std::string& column);

} // namespace index_catalog
} // namespace dbms
