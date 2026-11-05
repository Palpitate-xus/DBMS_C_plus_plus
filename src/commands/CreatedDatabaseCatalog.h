#pragma once
#include <string>
namespace dbms {
class StorageEngine;
// Only the creator of a brand-new database calls this, while holding its
// exclusive name/directory owner. Queries and cold metadata readers never do.
bool initializeCreatedDatabaseCatalog(const StorageEngine& engine,const std::string& database);
}
