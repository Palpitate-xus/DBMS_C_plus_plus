#pragma once
#include <filesystem>
#include <string>
namespace dbms {
class StorageEngine;
// Only the creator of a brand-new database calls this, while holding its
// exclusive name/directory owner. Queries and cold metadata readers never do.
bool initializeCreatedDatabaseCatalog(const StorageEngine& engine,const std::string& database);
// Publish a complete replacement of an existing catalog before mutable checkout.
bool upgradeExistingDescriptionDeclaration(const std::filesystem::path& database);
}
