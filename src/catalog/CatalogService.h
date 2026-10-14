#pragma once

#include "catalog/catalog.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dbms {

class StorageEngine;

// ============================================================================
// CatalogService: per-database CatalogManager cache
// ============================================================================
//
// CatalogManager is per-database (constructed with a dbPath). This service
// lazily creates one CatalogManager per database on first access and bootstraps
// system namespaces/types. It persists all cached catalogs on shutdown or
// explicit persistAll(). The catalog format is current-version only; storage
// files from an older release must be rebuilt from SQL instead of migrated.
//
// Thread-safety: actual database ownership precedes the service mutex and
// each CatalogManager's internal mutex. Engines keep independent models;
// clean committed peer changes replace a cached generation, not its rows.
//
class CatalogService {
public:
    explicit CatalogService(const StorageEngine& engine);
    ~CatalogService();

    CatalogService(const CatalogService&) = delete;
    CatalogService& operator=(const CatalogService&) = delete;

    // Get or create the CatalogManager for `dbname`.
    // On first creation: bootstraps system namespaces/types and loads the
    // current-version catalog files. This is a borrowed reference; retain
    // acquire()'s lease when rows must survive refresh/eviction/restore.
    CatalogManager& get(const std::string& dbname);
    // Owned generation lease: raw rows/references obtained from it remain
    // alive across another lookup's clean refresh or physical generation
    // replacement. Cold acquisition does not populate this service's cache.
    std::shared_ptr<CatalogManager> acquire(const std::string& dbname);

    // Remove the cached CatalogManager for `dbname` (e.g. on DROP DATABASE).
    // Retained leases remain alive. Eviction discards this service's model;
    // publication is explicit, not a retired generation's destructor effect.
    void evict(const std::string& dbname);

    // Persist all cached catalogs.
    bool persistAll();

    // Test/debug: is `dbname` currently cached?
    bool has(const std::string& dbname) const;
    CatalogManager::MetadataSnapshot metadataSnapshot(const std::string& dbname) const;
    std::map<std::string, std::string> domainColumns(const std::string& dbname,
                                                    const std::string& physicalTable) const;

    // Convert a physical storage name (schema__table or plain_table) to a
    // logical qualified name. This is the inverse of main.cpp's
    // resolveTableName() for schema-qualified names.
    static CatalogManager::QualifiedName logicalName(const std::string& physical);

private:
    const StorageEngine& engine_;
    mutable std::mutex mutex_;
    struct CachedCatalog {
        std::shared_ptr<CatalogManager> manager;
        uint64_t databaseDevice = 0;
        uint64_t databaseInode = 0;
        std::shared_ptr<CatalogManager> committed;
        uint64_t writeTransaction = 0;
    };
    std::unordered_map<std::string, CachedCatalog> cache_;
    std::shared_ptr<CatalogManager> acquireImpl(const std::string& dbname,
                                              bool cacheCold);
};

} // namespace dbms
