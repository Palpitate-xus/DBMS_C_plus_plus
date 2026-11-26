#pragma once

#include "catalog/catalog.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

struct Session;

namespace dbms {

class StorageEngine;
struct ExprValue;

// ============================================================================
// CatalogService: per-database CatalogManager cache
// ============================================================================
//
// CatalogManager is per-database (constructed with a dbPath). This service
// lazily creates one CatalogManager per database on first access and bootstraps
// system namespaces/types. It persists all cached catalogs on shutdown or
// explicit persistAll(). Mutable startup performs only the checked legacy
// upgrades implemented here; cold reads expose actual persisted metadata.
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

    // Persist cached catalogs; a nonempty database restricts publication to
    // that database. BEGIN must not publish another database's dirty model.
    bool persistAll(const std::string& database = {});

    // Transaction admission can retry a busy publisher after releasing its
    // database lock. A failed publication must still propagate as an error.
    CatalogManager::PersistResult persistCatalogs(const std::string& database);

    // Test/debug: is `dbname` currently cached?
    bool has(const std::string& dbname) const;
    // Public callers receive a complete snapshot by default. Internal callers
    // that need only declarations can explicitly omit the object graph.
    CatalogManager::MetadataSnapshot metadataSnapshot(const std::string& dbname,
                                                      bool includeDefaultObjects = true) const;
    // Owned namespace rows, with the transaction and publication checks used
    // by metadataSnapshot. The complete public snapshot remains unchanged.
    std::vector<PgNamespaceRow> namespaceSnapshot(const std::string& dbname) const;
    std::optional<PgNamespaceRow> namespaceIdentity(const std::string& dbname, Oid oid) const;
    std::optional<PgNamespaceRow> namespaceIdentity(const std::string& dbname, const std::string& name) const;
    CatalogManager::NamespaceIdentityNames namespaceIdentityNames(const std::string& dbname) const;
    std::optional<std::string> description(const std::string& dbname, Oid object,
        int32_t subobject, std::optional<std::string_view> catalogName) const;
    std::vector<PgDescriptionRow> descriptionSnapshot(const std::string& dbname,
        std::optional<Oid> object = std::nullopt) const;
    size_t scanDescriptionKeys(const std::string& dbname, uint64_t first, uint64_t end,
                               const CatalogManager::DescriptionKeyVisitor& visit) const;
    std::pair<Oid, std::string> resolveRegclass(const std::string& database, std::optional<Oid> oid,
        const std::string& input, const Session* session, bool textInput = false) const;
    std::optional<bool> relationIsVisible(const std::string& dbname, Oid oid,
                                         const std::vector<std::string>& searchPath) const;
    std::optional<std::string> indexDefinition(const std::string& database, Oid oid,
                                              int32_t column, bool pretty, const Session* session) const;
    std::vector<std::vector<ExprValue>> indexRows(const std::string& database,
        std::optional<Oid> owner, std::optional<Oid> index, bool includePredicate = false) const;
    std::optional<std::string> indexPredicateExpression(const std::string& database, Oid relation,
        std::string_view node, bool pretty) const;
    // Specialized read-only projection for pg_attrdef/pg_depend scans.
    CatalogManager::DefaultCatalogSnapshot defaultCatalogSnapshot(
        const std::string& dbname, CatalogManager::DefaultCatalogKind kind, bool includeExpressions = false) const;
    CatalogManager::ConstraintIdentities constraintIdentities(const std::string& dbname, std::optional<Oid> owner = {},
        std::optional<Oid> referenced = {}, std::optional<Oid> constraint = {}) const;
    std::vector<PgAuthIdRow> publicRoleSnapshot(std::optional<Oid> role) const;
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
    struct ReadCatalog {
        std::shared_ptr<CatalogManager> manager;
        std::string path;
    };
    ReadCatalog readCatalog(const std::string& dbname) const;
    // The caller supplies the result of its held database read fence.
    ReadCatalog readCatalog(const std::string& dbname, bool mayReadCurrentFiles) const;
    static std::vector<PgAttributeRow> relationAttributes(const ReadCatalog& catalog, Oid relation);
    friend std::vector<ExprValue> readPartitionAncestors(const StorageEngine&,
        const std::string&, const Session*, const ExprValue&);
    std::shared_ptr<CatalogManager> acquireImpl(const std::string& dbname,
                                              bool cacheCold);
};

} // namespace dbms
