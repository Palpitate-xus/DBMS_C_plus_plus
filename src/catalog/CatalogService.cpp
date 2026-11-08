#include "catalog/SystemAttributeDeclaration.h"
#include "catalog/BuiltinTypeProperties.h"
#include "catalog/SystemCatalogCollation.h"
#include "catalog/PgDefaultCatalog.h"
#include "catalog/CatalogService.h"
#include "catalog/PgStatsCatalog.h"
#include "catalog/PgClassCatalog.h"
#include "catalog/PgSequenceCatalog.h"
#include "catalog/SystemCatalogDeclaration.h"
#include "commands/CreatedDatabaseCatalog.h"
#include "process/StatsPersistence.h"
#include "commands/TableManage.h"
#include "commands/SequenceStorageName.h"
#include "utils/Session.h"
#include "common/DbError.h"
#include "access/IndexFileUtil.h"
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace dbms {

namespace fs = std::filesystem;

CatalogService::CatalogService(const StorageEngine& engine) : engine_(engine) {}

// The owning engine publishes explicitly while its ownership context is
// alive. Retired generations must never republish from destructor order.
CatalogService::~CatalogService() = default;

static fs::path catalogDirForDb(const StorageEngine& engine, const std::string& dbname) {
    return engine.dbPath(dbname) / "pg_catalog";
}

static void migrateLegacyPublicDottedSequences(
    const StorageEngine& engine, const std::string& dbname,
    const CatalogManager& catalog) {
    const fs::path directory = engine.dbPath(dbname);
    for (const auto& relation : catalog.listClasses()) {
        if (relation.relkind != 'S' ||
            relation.relname.find('.') == std::string::npos) {
            continue;
        }
        const auto nameSpace =
            catalog.getNamespace(relation.relnamespace);
        if (!nameSpace || nameSpace->nspname != "public") continue;

        const std::string encoded =
            sequenceStorageName("public", relation.relname);
        const fs::path upgradedPath = directory / (encoded + ".seqv2");
        if (fs::exists(upgradedPath)) continue;

        // The old sequencePath helper also stripped a leading "public."
        // from the joined key, even when it was part of a quoted relation
        // name rather than a namespace qualifier.
        const std::string legacyStorageName =
            relation.relname.rfind("public.", 0) == 0
                ? relation.relname.substr(7) : relation.relname;
        const fs::path legacyPath =
            directory / (legacyStorageName + ".seq");
        std::error_code error;
        const fs::file_status legacyStatus =
            fs::symlink_status(legacyPath, error);
        if (error) {
            throw std::runtime_error(
                "cannot inspect legacy sequence file " +
                legacyPath.string() + ": " + error.message());
        }
        if (!fs::exists(legacyStatus)) continue;
        if (!fs::is_regular_file(legacyStatus)) {
            throw std::runtime_error(
                "legacy sequence path is not a regular file: " +
                legacyPath.string());
        }

        // An old catalog should never contain both relations: they used the
        // same file and the second CREATE was rejected. If it does, do not
        // guess which sequence owns the bytes.
        const size_t dot = relation.relname.find('.');
        const auto conflictingNamespace =
            catalog.getNamespaceByName(relation.relname.substr(0, dot));
        const auto conflictingRelation = conflictingNamespace
            ? catalog.getClassByName(relation.relname.substr(dot + 1),
                                      conflictingNamespace->oid)
            : std::nullopt;
        // A non-public table uses <schema>__<table>.seq, not the joined
        // sequence key. A public table does use <table>.seq for identity
        // counters, so keep that case fail-closed when names overlap.
        if (conflictingRelation &&
            (conflictingRelation->relkind == 'S' ||
             (conflictingNamespace->nspname == "public" &&
              (conflictingRelation->relkind == 'r' ||
               conflictingRelation->relkind == 'p')))) {
            throw std::runtime_error(
                "ambiguous legacy sequence file ownership: " +
                legacyPath.string());
        }

        fs::rename(legacyPath, upgradedPath, error);
        if (error) {
            throw std::runtime_error(
                "cannot migrate legacy sequence file " +
                legacyPath.string() + ": " + error.message());
        }
        const int dirFd = ::open(directory.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        const bool durable = dirFd >= 0 && ::fsync(dirFd) == 0;
        if (dirFd >= 0) ::close(dirFd);
        if (!durable) {
            std::error_code rollbackError;
            fs::rename(upgradedPath, legacyPath, rollbackError);
            throw std::runtime_error(
                "cannot durably migrate legacy sequence file " +
                legacyPath.string());
        }
    }
}

namespace {
constexpr char physicalArrayVersion[] = "DBMS_PHYSICAL_BUILTIN_ARRAY_ATTRIBUTES1\n";
bool physicalArraysPublished(const fs::path& directory) {
    const auto path = directory / ".physical_builtin_array_attributes_version";
    std::error_code error;
    const auto status = fs::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory ||
        (!error && !fs::exists(status))) return false;
    if (error) throw DbError("58030", "cannot inspect physical array version");
    if (!fs::is_regular_file(status))
        throw DbError("XX001", "invalid physical array version");
    std::ifstream input(path, std::ios::binary);
    if (!input) throw DbError("58030", "cannot read physical array version");
    const std::string bytes((std::istreambuf_iterator<char>(input)), {});
    if (input.bad() || bytes != physicalArrayVersion)
        throw DbError("XX001", "invalid physical array version");
    return true;
}

std::vector<CatalogManager::PhysicalArrayAttribute> physicalArrayAttributes(
    const StorageEngine& engine, const std::string& database,
    const CatalogManager& catalog) {
    std::vector<CatalogManager::PhysicalArrayAttribute> fields;
    for (const auto& relation : catalog.listClasses()) {
        if (relation.relnamespace == 11 ||
            (relation.relkind != 'r' && relation.relkind != 'p')) continue;
        const auto nameSpace = catalog.getNamespace(relation.relnamespace);
        if (!nameSpace) throw DbError("XX001", "physical table has no namespace");
        const auto physicalName = nameSpace->nspname == "public" ? relation.relname
            : nameSpace->nspname + "__" + relation.relname;
        const auto schema = engine.readPersistedSchemaForCatalog(database, physicalName);
        // A catalog-only declaration is not evidence of physical array storage.
        if (!schema) continue;
        for (size_t i = 0; i < schema->len; ++i) {
            const auto& column = schema->cols[i];
            if (!column.isArray || !column.domainName.empty() || !column.enumValues.empty())
                continue;
            // Older physical schemas can omit the declared domain identity.
            // Its real catalog owner takes precedence over a base codec name.
            const auto attribute = catalog.getAttribute(relation.oid, column.dataName);
            const auto logicalType = attribute ? catalog.getType(attribute->atttypid) : std::nullopt;
            if (logicalType && logicalType->typnamespace != 11) continue;
            std::string elementName = column.dataType;
            while (elementName.size() >= 2 &&
                   elementName.compare(elementName.size() - 2, 2, "[]") == 0)
                elementName.resize(elementName.size() - 2);
            // Match the existing DDL owner for legacy physical codec spellings.
            const Oid element = elementName == "float" && column.dsize == sizeof(float) ? 700
                : elementName == "double" && column.dsize == sizeof(double) ? 701
                : elementName == "blob" ? 17 : mapBuiltinTypeNameToOid(elementName);
            if (!element) continue; // User-defined type identity has its own owner.
            fields.push_back({relation.oid, static_cast<int16_t>(i + 1),
                column.dataName, element, column.collation.empty()});
        }
    }
    return fields;
}

struct CatalogDatabaseIdentity { uint64_t device = 0; uint64_t inode = 0; };
bool databaseIdentity(const std::filesystem::path& path, CatalogDatabaseIdentity& identity,
                      bool reservedCatalogOnly = false) {
    struct stat data{};
    if (::stat(path.c_str(), &data) != 0 || !S_ISDIR(data.st_mode)) return false;
    struct stat marker{};
    if (::lstat((path / ".dbms_physical_backup").c_str(), &marker) == 0 || errno != ENOENT)
        return false;
    if (!reservedCatalogOnly) {
        struct stat list{};
        if (::lstat((path / "tlist.lst").c_str(), &list) != 0 || !S_ISREG(list.st_mode))
            return false;
    }
    identity = {static_cast<uint64_t>(data.st_dev), static_cast<uint64_t>(data.st_ino)};
    return true;
}

std::shared_ptr<CatalogManager> loadOwnedCatalog(
    const StorageEngine& engine, const std::string& database) {
    const auto directory = catalogDirForDb(engine, database);
    // Allocated system view/row-type OIDs must be published by one actual
    // startup/DDL owner before another engine can initialize the same image.
    // Read-only metadata snapshots never enter this mutable bootstrap path.
    fs::create_directories(directory);
    StatsFileLock bootstrap(directory/".system_view_bootstrap");
    if(!bootstrap.ok())throw DbError("58030","could not lock system view initialization");
    struct stat namespaceFile{};
    const bool newNamespaceCatalog =
        ::lstat((directory / "pg_namespace.cat").c_str(), &namespaceFile) != 0;
    if (newNamespaceCatalog && errno != ENOENT)
        throw DbError("58030", "cannot inspect namespace catalog bootstrap state");
    auto catalog = std::make_shared<CatalogManager>(directory.string(), false);
    catalog->bootstrapSystemNamespaces();
    const bool publishedTypeProperties=builtin_type_properties::published(directory);
    if(publishedTypeProperties)(void)catalog->bootstrapBuiltinTypeProperties(true);
    catalog->bootstrapSystemTypes();
    const bool initializedTypeProperties=!publishedTypeProperties && catalog->bootstrapBuiltinTypeProperties(false);
    const bool publishedCatalogCollations=system_catalog_collation::published(directory);
    const bool initializedCatalogCollations=catalog->bootstrapSystemCatalogCollations(publishedCatalogCollations);
    const auto systemMarker=directory/".owned_system_catalog_version";
    constexpr char systemVersion[]="DBMS_SYSTEM_CATALOG_DECLS1\n";
    struct stat systemStatus{};
    const bool publishedSystemCatalog=::lstat(systemMarker.c_str(),&systemStatus)==0;
    if(!publishedSystemCatalog && errno!=ENOENT)throw DbError("58030","cannot inspect system catalog version");
    if(publishedSystemCatalog) {
        if(!S_ISREG(systemStatus.st_mode) || systemStatus.st_size!=sizeof(systemVersion)-1)
            throw DbError("XX001","invalid system catalog version");
        std::ifstream input(systemMarker,std::ios::binary);
        if(!input)throw DbError("58030","cannot read system catalog version");
        const std::string bytes((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
        if(input.bad() || bytes!=systemVersion || !catalog->getClass(PgClassOid_Class))
            throw DbError("XX001","invalid published system catalog declaration");
    }
    const bool initializedClassCatalog=class_catalog::bootstrapRelation(*catalog);
    const bool initializedOwnedCatalogs=system_catalog::bootstrapRelations(*catalog,publishedSystemCatalog);
    const bool publishedDefaultCatalog = default_catalog::published(directory);
    const bool initializedDefaultCatalog = default_catalog::bootstrap(*catalog, publishedDefaultCatalog);
    const auto sequenceMarker=directory/".sequence_catalog_version";
    constexpr char sequenceVersion[]="DBMS_PG_SEQUENCE_CATALOG1\n";
    struct stat markerStatus{};
    const bool publishedSequenceCatalog=::lstat(sequenceMarker.c_str(),&markerStatus)==0;
    if(!publishedSequenceCatalog && errno!=ENOENT)
        throw DbError("58030","cannot inspect sequence catalog version");
    if(publishedSequenceCatalog) {
        if(!S_ISREG(markerStatus.st_mode) || markerStatus.st_size!=sizeof(sequenceVersion)-1)
            throw DbError("XX001","invalid sequence catalog version");
        std::ifstream input(sequenceMarker,std::ios::binary);
        if(!input)throw DbError("58030","cannot read sequence catalog version");
        const std::string bytes((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
        if(input.bad() || bytes!=sequenceVersion)
            throw DbError("XX001","invalid sequence catalog version");
    }
    const bool initializedSequenceCatalog=sequence_catalog::bootstrapRelation(*catalog,publishedSequenceCatalog);
    const bool initializedStatisticsView=analyzed_statistics::bootstrapView(*catalog);
    const bool publishedSystemAttributes=system_attributes::published(directory);
    const bool initializedSystemAttributes=catalog->bootstrapSystemAttributes(publishedSystemAttributes);
    (void)catalog->bootstrapSystemCatalogCollations(true);
    const bool publishedPhysicalArrays = physicalArraysPublished(directory);
    const bool initializedPhysicalArrays = catalog->bootstrapPhysicalArrayAttributes(
        physicalArrayAttributes(engine, database, *catalog), publishedPhysicalArrays);
    // Some legitimate server cluster scaffolds already have tlist.lst and
    // auth metadata, but no namespace catalog. A newly bootstrapped logical
    // public namespace must have the same checked physical marker as CREATE
    // DATABASE. Never repair a loaded DROP PUBLIC image, nor promote the
    // historical directory-only reserved auth catalog into a database.
    if (newNamespaceCatalog && engine.databaseExists(database) &&
        catalog->findNamespaceByName("public")) {
        const auto marker = engine.dbPath(database) / ".schema_public";
        std::error_code error;
        const auto status = fs::symlink_status(marker, error);
        if (error && error != std::errc::no_such_file_or_directory)
            throw DbError("58030", "cannot inspect public namespace bootstrap marker");
        if (fs::exists(status)) {
            if (!fs::is_regular_file(status))
                throw DbError("58030", "public namespace bootstrap marker is not a regular file");
        } else {
            const auto result = index_file::writeAtomicallyNoReplace(marker, "");
            if (result != index_file::WriteResult::OK &&
                !(result == index_file::WriteResult::ALREADY_EXISTS &&
                  engine.schemaExists(database, "public")))
                throw DbError("58030", "cannot publish public namespace bootstrap marker");
        }
    }
    migrateLegacyPublicDottedSequences(engine, database, *catalog);
    if((initializedClassCatalog || initializedOwnedCatalogs || initializedSequenceCatalog || initializedStatisticsView || initializedSystemAttributes || initializedTypeProperties || initializedCatalogCollations || initializedPhysicalArrays || initializedDefaultCatalog || !publishedDefaultCatalog || !publishedTypeProperties || !publishedCatalogCollations || !publishedPhysicalArrays) && !catalog->persistAll())
        throw DbError("58030","could not publish initialized system view identities");
    if(!publishedSequenceCatalog &&
       index_file::writeAtomicallyNoReplace(sequenceMarker,sequenceVersion)!=index_file::WriteResult::OK)
        throw DbError("58030","could not publish sequence catalog version");
    if(!publishedSystemCatalog &&
       index_file::writeAtomicallyNoReplace(systemMarker,systemVersion)!=index_file::WriteResult::OK)
        throw DbError("58030","could not publish system catalog version");
    if(!publishedSystemAttributes && index_file::writeAtomicallyNoReplace(directory/".system_attribute_version",
       system_attributes::version)!=index_file::WriteResult::OK)
        throw DbError("58030","could not publish system attribute version");
    if(!publishedTypeProperties && index_file::writeAtomicallyNoReplace(directory/".builtin_type_properties_version",
       builtin_type_properties::version)!=index_file::WriteResult::OK)
        throw DbError("58030","could not publish builtin type property version");
    if(!publishedCatalogCollations && index_file::writeAtomicallyNoReplace(directory/".system_catalog_collation_version",
       system_catalog_collation::version)!=index_file::WriteResult::OK)
        throw DbError("58030","could not publish system catalog collation version");
    if (!publishedPhysicalArrays && index_file::writeAtomicallyNoReplace(
            directory / ".physical_builtin_array_attributes_version", physicalArrayVersion)
            != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish physical array version");
    if (!publishedDefaultCatalog && index_file::writeAtomicallyNoReplace(
            directory / ".attribute_default_catalog_version", default_catalog::version)
            != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish attribute default catalog version");
    return catalog;
}
}

bool initializeCreatedDatabaseCatalog(const StorageEngine& engine,const std::string& database) {
    // The database creator owns the exclusive name lock. Acquiring a normal
    // catalog read lease here would conflict with that same creation owner.
    // Publish the initialized private catalog before CREATE reports success;
    // keep the mutable service cache empty for subsequent metadata-only reads.
    if(fs::exists(catalogDirForDb(engine,database)))return false;
    const auto catalog=loadOwnedCatalog(engine,database);
    return catalog->persistAll();
}

CatalogManager& CatalogService::get(const std::string& dbname) {
    return *acquireImpl(dbname, true);
}

std::shared_ptr<CatalogManager> CatalogService::acquire(const std::string& dbname) {
    return acquireImpl(dbname, false);
}

std::shared_ptr<CatalogManager> CatalogService::acquireImpl(
    const std::string& dbname, bool cacheCold) {
    // Actual database ownership precedes the service/model mutex. A durable
    // file publication during a foreign parent transaction is not COMMIT.
    const auto ownership = engine_.catalogReadOwnership(dbname);
    CatalogDatabaseIdentity physical;
    if (!databaseIdentity(engine_.dbPath(dbname), physical, dbname == "info"))
        throw DbError("3D000", "catalog database does not exist");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto selectOwnedGeneration = [&](CachedCatalog& cached) {
        if (!ownership.writeTransaction) {
            cached.committed.reset();
            cached.writeTransaction = 0;
        } else if (cached.writeTransaction != ownership.writeTransaction) {
            // BEGIN persisted the pre-transaction model before taking its
            // physical image. Fork before this writer can mutate it: readers
            // on the same engine need that committed model, not the writer's
            // already-published but uncommitted catalog files.
            if (cached.manager->hasUnpublishedChanges())
                throw DbError("58030", "catalog write checkout has unpublished metadata");
            auto writable = loadOwnedCatalog(engine_, dbname);
            cached.committed = cached.manager;
            cached.manager = std::move(writable);
            cached.writeTransaction = ownership.writeTransaction;
        }
        return cached.manager;
    };
    const auto found = cache_.find(dbname);
    const bool sameGeneration = found != cache_.end() &&
        found->second.databaseDevice == physical.device &&
        found->second.databaseInode == physical.inode;
    if (sameGeneration) {
        if (!ownership.mayReadCurrentFiles)
            return found->second.committed ? found->second.committed : found->second.manager;
        const auto state = found->second.manager
            ? found->second.manager->publicationState()
            : CatalogManager::PublicationState::CleanPeerChange;
        if (state == CatalogManager::PublicationState::Current)
            return selectOwnedGeneration(found->second);
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "catalog refresh conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold catalog read conflicts with an active DDL owner");
    auto replacement = loadOwnedCatalog(engine_, dbname);
    // A cold mutable bootstrap has no preceding cached model for BEGIN to
    // publish. Establish its builtin image under the actual writer before
    // checking out a separate mutable generation.
    if (ownership.writeTransaction && replacement->hasUnpublishedChanges() &&
        !replacement->persistAll())
        throw DbError("58030", "cannot publish initial catalog generation");
    // Copy-on-refresh preserves existing owned generations and their rows.
    if (cacheCold || found != cache_.end()) {
        std::shared_ptr<CatalogManager> committed;
        if (found != cache_.end() && ownership.writeTransaction &&
            found->second.writeTransaction == ownership.writeTransaction)
            committed = found->second.committed;
        auto& cached = cache_[dbname];
        cached = {replacement, physical.device, physical.inode, committed,
                  committed ? ownership.writeTransaction : 0};
        if (ownership.writeTransaction)
            return selectOwnedGeneration(cached);
    }
    return replacement;
}

void CatalogService::evict(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = cache_.find(dbname);
    if (found != cache_.end() && found->second.committed &&
        engine_.inTransaction() &&
        found->second.writeTransaction == engine_.currentTxnId()) {
        // SAVEPOINT/statement restore evicts the writer's physical image,
        // not the parent's committed generation seen by other backends.
        found->second.manager.reset();
        CatalogDatabaseIdentity physical;
        if (databaseIdentity(engine_.dbPath(dbname), physical, dbname == "info")) {
            found->second.databaseDevice = physical.device;
            found->second.databaseInode = physical.inode;
        }
        return;
    }
    cache_.erase(dbname);
}

bool CatalogService::persistAll(const std::string& database) {
    std::vector<std::pair<std::string, CachedCatalog>> models;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : cache_)
            if(database.empty() || engine_.dbPath(entry.first).lexically_normal()==
                                   engine_.dbPath(database).lexically_normal())models.push_back(entry);
    }
    bool ok = true;
    for (const auto& entry : models) {
        // Embedded callers and tests may remove a database directory outside
        // DROP DATABASE. Never let an orphaned catalog cache make an
        // unrelated checkpoint or transaction fail, and never recreate the
        // removed database from stale in-memory metadata.
        if (!engine_.databaseExists(entry.first) && entry.first != "info") {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto current = cache_.find(entry.first);
            if (current != cache_.end() && current->second.manager == entry.second.manager)
                cache_.erase(current);
            continue;
        }
        const auto ownership = engine_.catalogReadOwnership(entry.first);
        CatalogDatabaseIdentity physical;
        if (!databaseIdentity(engine_.dbPath(entry.first), physical, entry.first == "info")) {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto current = cache_.find(entry.first);
            if (current != cache_.end() && current->second.manager == entry.second.manager)
                cache_.erase(current);
            continue;
        }
        if (physical.device != entry.second.databaseDevice || physical.inode != entry.second.databaseInode) {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto current = cache_.find(entry.first);
            if (current != cache_.end() && current->second.manager == entry.second.manager)
                cache_.erase(current);
            continue;
        }
        if (!ownership.mayReadCurrentFiles) {
            if (entry.second.manager && entry.second.manager->hasUnpublishedChanges()) ok = false;
            continue;
        }
        auto publishing = entry.second.manager;
        if (!publishing ||
            publishing->publicationState() == CatalogManager::PublicationState::CleanPeerChange) {
            // persistAll() on the old manager reloads its vectors in place.
            // A service refresh must instead keep an acquired generation's
            // rows stable, including BEGIN/checkpoint's implicit publication.
            auto replacement = loadOwnedCatalog(engine_, entry.first);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                const auto current = cache_.find(entry.first);
                if (current != cache_.end() && current->second.manager == publishing) {
                    const auto committed = current->second.writeTransaction == ownership.writeTransaction
                        ? current->second.committed : std::shared_ptr<CatalogManager>{};
                    current->second = {replacement, physical.device, physical.inode, committed,
                                      committed ? ownership.writeTransaction : 0};
                }
            }
            publishing = std::move(replacement);
        }
        if (!publishing->persistAll()) ok = false;
    }
    return ok;
}

bool CatalogService::has(const std::string& dbname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.find(dbname) != cache_.end();
}

CatalogManager::MetadataSnapshot CatalogService::metadataSnapshot(const std::string& dbname,
                                                                  bool includeDefaultObjects) const {
    const auto ownership = engine_.catalogReadOwnership(dbname);
    CatalogDatabaseIdentity physical;
    if (!databaseIdentity(engine_.dbPath(dbname), physical, dbname == "info")) return {};
    std::shared_ptr<CatalogManager> model;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = cache_.find(dbname);
        if (found != cache_.end() && found->second.databaseDevice == physical.device &&
            found->second.databaseInode == physical.inode)
            model = !ownership.mayReadCurrentFiles && found->second.committed
                ? found->second.committed : found->second.manager;
    }
    if (model) {
        if (!ownership.mayReadCurrentFiles) return model->metadataSnapshot(includeDefaultObjects);
        const auto state = model->publicationState();
        if (state == CatalogManager::PublicationState::Current) return model->metadataSnapshot(includeDefaultObjects);
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "catalog read conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold catalog read conflicts with an active DDL owner");
    // Copying committed metadata remains pure: no bootstrap/OID allocation,
    // cache insertion, legacy migration or destructor publication.
    return CatalogManager::readMetadataSnapshot(catalogDirForDb(engine_, dbname).string(), includeDefaultObjects);
}

CatalogManager::DefaultCatalogSnapshot CatalogService::defaultCatalogSnapshot(
    const std::string& dbname, CatalogManager::DefaultCatalogKind kind) const {
    const auto ownership = engine_.catalogReadOwnership(dbname);
    CatalogDatabaseIdentity physical;
    if (!databaseIdentity(engine_.dbPath(dbname), physical, dbname == "info")) return {};
    std::shared_ptr<CatalogManager> model;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = cache_.find(dbname);
        if (found != cache_.end() && found->second.databaseDevice == physical.device &&
            found->second.databaseInode == physical.inode)
            model = !ownership.mayReadCurrentFiles && found->second.committed
                ? found->second.committed : found->second.manager;
    }
    if (model) {
        if (!ownership.mayReadCurrentFiles) return model->defaultCatalogSnapshot(kind);
        const auto state = model->publicationState();
        if (state == CatalogManager::PublicationState::Current) return model->defaultCatalogSnapshot(kind);
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "catalog read conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold catalog read conflicts with an active DDL owner");
    return CatalogManager::readDefaultCatalogSnapshot(catalogDirForDb(engine_, dbname).string(), kind);
}

std::map<std::string, std::string> CatalogService::domainColumns(
    const std::string& dbname, const std::string& physicalTable) const {
    const auto logical = logicalName(physicalTable);
    const auto schema = logical.schema.empty() ? "public" : logical.schema;
    const auto ownership = engine_.catalogReadOwnership(dbname);
    CatalogDatabaseIdentity physical;
    if (!databaseIdentity(engine_.dbPath(dbname), physical, dbname == "info")) return {};
    std::shared_ptr<CatalogManager> model;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = cache_.find(dbname);
        if (found != cache_.end() && found->second.databaseDevice == physical.device &&
            found->second.databaseInode == physical.inode)
            model = !ownership.mayReadCurrentFiles && found->second.committed
                ? found->second.committed : found->second.manager;
    }
    if (model) {
        if (!ownership.mayReadCurrentFiles) return model->domainColumns(schema, logical.name);
        const auto state = model->publicationState();
        if (state == CatalogManager::PublicationState::Current) return model->domainColumns(schema, logical.name);
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "domain catalog read conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold domain catalog read conflicts with an active DDL owner");
    return CatalogManager::readDomainColumns(catalogDirForDb(engine_, dbname).string(), schema, logical.name);
}

CatalogManager::QualifiedName CatalogService::logicalName(const std::string& physical) {
    CatalogManager::QualifiedName qn;
    qn.schema.clear();
    qn.name = physical;

    if (const Session* session = currentSession()) {
        const std::string prefix = tempTablePrefix(*session, "");
        if (physical.rfind(prefix, 0) == 0) {
            const std::string name = physical.substr(prefix.size());
            if (session->tempTables.count(name) || session->transientTempTables.count(name)) {
                qn.schema = sessionTempSchemaName(*session);
                qn.name = name;
                return qn;
            }
        }
    }

    size_t pos = physical.find("__");
    if (pos != std::string::npos && pos > 0 && pos + 2 < physical.size()) {
        qn.schema = physical.substr(0, pos);
        qn.name = physical.substr(pos + 2);
    }
    return qn;
}

} // namespace dbms
