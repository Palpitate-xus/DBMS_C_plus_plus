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
#include "common/DatabaseMetadataWrite.h"
#include "catalog/PgInheritanceCatalog.h"
#include "catalog/PgAccessMethodCatalog.h"
#include "catalog/PgCollationCatalog.h"
#include "catalog/PgForeignKeyCatalog.h"
#include "catalog/DatumMetadataFactory.h"
#include "expression/regclass_input.h"
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
        for (size_t i = 0; i < schema->len(); ++i) {
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

bool catalogVersionPublished(const fs::path& path, std::string_view version) {
    std::error_code error;
    const auto status = fs::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory ||
        (!error && !fs::exists(status))) return false;
    if (error) throw DbError("58030", "cannot inspect catalog version: " + path.filename().string());
    if (!fs::is_regular_file(status) ||
        fs::file_size(path) != version.size())
        throw DbError("XX001", "invalid catalog version: " + path.filename().string());
    std::ifstream input(path, std::ios::binary);
    if (!input) throw DbError("58030", "cannot read catalog version: " + path.filename().string());
    const std::string bytes((std::istreambuf_iterator<char>(input)), {});
    if (input.bad() || bytes != version)
        throw DbError("XX001", "invalid catalog version: " + path.filename().string());
    return true;
}

void initializeColumnCollations(const StorageEngine& engine, const std::string& database,
                                CatalogManager& catalog) {
    std::unordered_map<std::string, Oid> physicalOwners;
    for (const auto& relation : catalog.listClasses()) {
        if (relation.relnamespace == 11 || (relation.relkind != 'r' && relation.relkind != 'p')) continue;
        const auto schema = catalog.getNamespace(relation.relnamespace);
        if (!schema || schema->nspname.rfind("pg_temp_", 0) == 0) continue;
        const std::string physical = schema->nspname == "public" ? relation.relname
            : schema->nspname + "__" + relation.relname;
        const auto [it, unique] = physicalOwners.emplace(physical, relation.oid);
        if (!unique) it->second = INVALID_OID;
    }
    for (const auto& [physical, owner] : physicalOwners) {
        if (!owner) continue;
        const auto schema = engine.readPersistedSchemaForCatalog(database, physical);
        if (!schema) continue;
        auto attributes = catalog.findUserAttributes(owner);
        bool changed = false;
        for (auto& attribute : attributes) {
            if (attribute.attisdropped || attribute.attnum <= 0 ||
                static_cast<size_t>(attribute.attnum) > schema->len()) continue;
            const auto& column = schema->cols[attribute.attnum - 1];
            if (column.dataName != attribute.attname) continue;
            const auto oid = collation_catalog::builtinOid(column.collation);
            if (!oid || oid == attribute.attcollation) continue;
            const auto type = catalog.getType(attribute.atttypid);
            if (!type || !type->typcollation) continue;
            attribute.attcollation = oid;
            changed = true;
        }
        if (changed && !catalog.replaceAttributes(owner, attributes))
            throw DbError("XX001", "could not publish column collation identities");
    }
}

void initializeRelationAccessMethods(const StorageEngine& engine, const std::string& database,
                                     CatalogManager& catalog) {
    const auto snapshot = catalog.metadataSnapshot();
    std::unordered_map<Oid, const PgClassRow*> relations;
    std::unordered_map<Oid, std::string_view> schemas;
    std::unordered_map<Oid, Oid> indexParents;
    for (const auto& relation : snapshot.relations) relations.emplace(relation.oid, &relation);
    for (const auto& schema : snapshot.namespaces) schemas.emplace(schema.oid, schema.nspname);
    for (const auto& edge : snapshot.dependencies) {
        if (edge.classid != PgClassOid_Class || edge.refclassid != PgClassOid_Class ||
            edge.objsubid || edge.refobjsubid || edge.deptype != 'a') continue;
        const auto [position, inserted] = indexParents.emplace(edge.objid, edge.refobjid);
        if (!inserted && position->second != edge.refobjid) position->second = INVALID_OID;
    }
    const auto physicalNames = [&](const PgClassRow& relation) {
        std::vector<std::string> names;
        const auto found = schemas.find(relation.relnamespace);
        if (found == schemas.end()) return names;
        const auto schema = found->second;
        if (schema == "public") {
            names = {relation.relname, "public__" + relation.relname};
        } else if (schema.substr(0, 8) == "pg_temp_") {
            names.push_back("__tmp_" + std::string(schema.substr(8)) + "_" + relation.relname);
        } else {
            names.push_back(std::string(schema) + "__" + relation.relname);
        }
        if (relation.relkind == 'm')
            for (auto& name : names) name = StorageEngine::materializedViewPrefix(name);
        return names;
    };
    struct StoredRelation {
        std::string name;
        bool usesHeap = false;
    };
    std::unordered_map<Oid, StoredRelation> storedRelations;
    const auto storedRelation = [&](const PgClassRow& relation) -> const StoredRelation& {
        auto [entry, inserted] = storedRelations.try_emplace(relation.oid);
        if (inserted) {
            for (const auto& name : physicalNames(relation)) {
                const auto schema = engine.readPersistedSchemaForCatalog(database, name);
                if (!schema) continue;
                entry->second = {name, schema->partitionType == TableSchema::PartitionType::None};
                break;
            }
        }
        return entry->second;
    };
    for (auto relation : snapshot.relations) {
        if (relation.relam) continue;
        if (relation.relkind == 'm' || (relation.relkind == 'r' && relation.relnamespace == 11)) {
            relation.relam = access_method_catalog::heapOid;
        } else if (relation.relkind == 'r') {
            if (storedRelation(relation).usesHeap) relation.relam = access_method_catalog::heapOid;
        } else if (relation.relkind == 'i') {
            const auto parent = indexParents.find(relation.oid);
            if (parent == indexParents.end()) continue;
            const auto table = relations.find(parent->second);
            if (table == relations.end()) continue;
            const auto& name = storedRelation(*table->second).name;
            if (!name.empty()) {
                const auto index = engine.getNamedIndex(database, name, relation.relname);
                if (index)
                    relation.relam = access_method_catalog::indexOid(index->accessMethod);
            }
        }
        if (relation.relam && !catalog.updateClass(relation.oid, relation))
            throw DbError("XX001", "relation disappeared during access method catalog upgrade");
    }
}

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
    const auto inheritanceMarker = directory / ".inheritance_catalog_version";
    std::error_code inheritanceError;
    const auto inheritanceStatus = fs::symlink_status(inheritanceMarker, inheritanceError);
    const bool publishedInheritance = fs::exists(inheritanceStatus);
    if (inheritanceError && inheritanceError != std::errc::no_such_file_or_directory)
        throw DbError("58030", "could not inspect inheritance catalog version");
    if (publishedInheritance) {
        if (!fs::is_regular_file(inheritanceStatus))
            throw DbError("XX001", "invalid inheritance catalog version");
        std::ifstream input(inheritanceMarker, std::ios::binary);
        if (!input) throw DbError("58030", "could not read inheritance catalog version");
        const std::string bytes((std::istreambuf_iterator<char>(input)), {});
        if (input.bad() || bytes != inheritance_catalog::version)
            throw DbError("XX001", "invalid inheritance catalog version");
    }
    const bool initializedInheritance = default_catalog::bootstrapOne(*catalog,
        inheritance_catalog::relationOid, inheritance_catalog::name, inheritance_catalog::fields, publishedInheritance);
    const auto accessMethodMarker = directory / ".access_method_catalog_version";
    const bool publishedAccessMethods = catalogVersionPublished(accessMethodMarker, access_method_catalog::version);
    const bool initializedAccessMethods = default_catalog::bootstrapOne(*catalog,
        access_method_catalog::relationOid, access_method_catalog::name, access_method_catalog::fields,
        publishedAccessMethods);
    const auto collationMarker = directory / ".collation_catalog_version";
    const bool publishedCollations = catalogVersionPublished(collationMarker, collation_catalog::version);
    const bool initializedCollations = default_catalog::bootstrapOne(*catalog,
        collation_catalog::relationOid, collation_catalog::name, collation_catalog::fields, publishedCollations);
    if (initializedCollations) {
        auto declaration = catalog->getClass(collation_catalog::relationOid).value();
        declaration.relam = access_method_catalog::heapOid;
        if (!catalog->updateClass(declaration.oid, declaration))
            throw DbError("XX001", "could not publish collation catalog relation");
    }
    const bool publishedDefaultCatalog = default_catalog::published(directory);
    const bool initializedDefaultCatalog = default_catalog::bootstrap(*catalog, publishedDefaultCatalog);
    const auto foreignKeyMarker = directory / ".foreign_key_catalog_version";
    const bool publishedForeignKeys = catalogVersionPublished(foreignKeyMarker, foreign_key_catalog::version);
    const bool initializedForeignKeys = default_catalog::bootstrapOne(*catalog,
        foreign_key_catalog::relationOid, foreign_key_catalog::name, foreign_key_catalog::fields, publishedForeignKeys);
    if (publishedForeignKeys && !fs::is_regular_file(directory / "pg_constraint.cat"))
        throw DbError("58030", "published foreign key identity file is missing");
    if (!publishedForeignKeys) {
        for (const auto& relation : catalog->listClasses()) {
            if (relation.relkind != 'r' || relation.relnamespace == 11 || relation.relpersistence == 't') continue;
            const auto space = catalog->getNamespace(relation.relnamespace);
            if (!space) continue;
            const auto physical = space->nspname == "public" ? relation.relname : space->nspname + "__" + relation.relname;
            foreign_key_catalog::synchronize(engine, database, *catalog, relation, physical);
        }
    }
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
    if (!publishedAccessMethods) initializeRelationAccessMethods(engine, database, *catalog);
    if (!publishedCollations) initializeColumnCollations(engine, database, *catalog);
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
    if((initializedClassCatalog || initializedOwnedCatalogs || initializedInheritance || initializedSequenceCatalog || initializedStatisticsView || initializedSystemAttributes || initializedTypeProperties || initializedCatalogCollations || initializedPhysicalArrays || initializedDefaultCatalog || !publishedDefaultCatalog || !publishedTypeProperties || !publishedCatalogCollations || !publishedPhysicalArrays || initializedAccessMethods || !publishedAccessMethods || !publishedCollations || initializedForeignKeys || !publishedForeignKeys) && !catalog->persistAll())
        throw DbError("58030","could not publish initialized system view identities");
    if (!publishedForeignKeys && index_file::writeAtomicallyNoReplace(foreignKeyMarker,
            foreign_key_catalog::version) != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish foreign key catalog version");
    if (!publishedCollations && index_file::writeAtomicallyNoReplace(collationMarker,
            collation_catalog::version) != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish collation catalog version");
    if (!publishedAccessMethods && index_file::writeAtomicallyNoReplace(accessMethodMarker,
            access_method_catalog::version) != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish access method catalog version");
    if (!publishedInheritance && index_file::writeAtomicallyNoReplace(inheritanceMarker,
            inheritance_catalog::version) != index_file::WriteResult::OK)
        throw DbError("58030", "could not publish inheritance catalog version");
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
    return persistCatalogs(database) == CatalogManager::PersistResult::Published;
}

CatalogManager::PersistResult CatalogService::persistCatalogs(const std::string& database) {
    using Result = CatalogManager::PersistResult;
    std::vector<std::pair<std::string, CachedCatalog>> models;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : cache_)
            if(database.empty() || engine_.dbPath(entry.first).lexically_normal()==
                                   engine_.dbPath(database).lexically_normal())models.push_back(entry);
    }
    auto status = Result::Published;
    const auto record = [&](Result result) {
        if (result == Result::Failed || status == Result::Published) status = result;
    };
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
            if (entry.second.manager && entry.second.manager->hasUnpublishedChanges()) record(Result::Failed);
            continue;
        }
        if (entry.second.manager) {
            const auto result = entry.second.manager->persistCurrent();
            if (result != Result::RefreshRequired) {
                record(result);
                continue;
            }
        }

        // A clean peer refresh must keep every acquired generation stable.
        // Publish a private replacement, then make it available to readers.
        const DatabaseMetadataWrite writer(engine_.dbPath(entry.first));
        if (!writer.acquired()) {
            record(writer.contended() ? Result::Busy : Result::Failed);
            continue;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        const auto current = cache_.find(entry.first);
        if (current == cache_.end()) continue;
        auto& cached = current->second;
        if (cached.databaseDevice != physical.device || cached.databaseInode != physical.inode) {
            record(Result::Failed);
            continue;
        }
        CatalogDatabaseIdentity refreshed;
        if (!databaseIdentity(engine_.dbPath(entry.first), refreshed, entry.first == "info") ||
            refreshed.device != physical.device || refreshed.inode != physical.inode) {
            record(Result::Failed);
            continue;
        }
        // Another publisher may have changed the file set before this
        // refresh acquired the metadata owner. Revalidate on this rare path.
        if (cached.manager) {
            const auto result = cached.manager->persistCurrent();
            if (result != Result::RefreshRequired) {
                record(result);
                continue;
            }
        }
        auto replacement = loadOwnedCatalog(engine_, entry.first);
        if (replacement->persistCurrent() != Result::Published ||
            (cached.manager && cached.manager->hasUnpublishedChanges())) {
            record(Result::Failed);
            continue;
        }
        const auto committed = cached.writeTransaction == ownership.writeTransaction
            ? cached.committed : std::shared_ptr<CatalogManager>{};
        cached = {std::move(replacement), physical.device, physical.inode, committed,
                  committed ? ownership.writeTransaction : 0};
    }
    return status;
}

bool CatalogService::has(const std::string& dbname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.find(dbname) != cache_.end();
}

CatalogService::ReadCatalog CatalogService::readCatalog(const std::string& dbname) const {
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
        if (!ownership.mayReadCurrentFiles) return {std::move(model), {}};
        const auto state = model->publicationState();
        if (state == CatalogManager::PublicationState::Current) return {std::move(model), {}};
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "catalog read conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold catalog read conflicts with an active DDL owner");
    // Copying committed metadata remains pure: no bootstrap/OID allocation,
    // cache insertion, legacy migration or destructor publication.
    return {{}, catalogDirForDb(engine_, dbname).string()};
}

CatalogManager::MetadataSnapshot CatalogService::metadataSnapshot(const std::string& dbname,
                                                                  bool includeDefaultObjects) const {
    const auto catalog = readCatalog(dbname);
    if (catalog.manager) return catalog.manager->metadataSnapshot(includeDefaultObjects);
    if (catalog.path.empty()) return {};
    return CatalogManager::readMetadataSnapshot(catalog.path, includeDefaultObjects);
}

CatalogManager::ForeignKeyIdentities CatalogService::foreignKeyIdentities(const std::string& database,
    std::optional<Oid> constraint) const {
    const auto catalog = readCatalog(database);
    if (catalog.manager) return catalog.manager->foreignKeyIdentities(constraint);
    if (catalog.path.empty()) return {};
    return CatalogManager::readForeignKeyIdentities(catalog.path, constraint);
}

std::optional<bool> CatalogService::relationIsVisible(
    const std::string& dbname, Oid oid, const std::vector<std::string>& searchPath) const {
    const auto catalog = readCatalog(dbname);
    if (catalog.manager) return catalog.manager->relationIsVisible(oid, searchPath);
    if (catalog.path.empty()) return std::nullopt;
    return CatalogManager::readRelationVisibility(catalog.path, oid, searchPath);
}

std::vector<PgNamespaceRow> CatalogService::namespaceSnapshot(const std::string& dbname) const {
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
        if (!ownership.mayReadCurrentFiles) return model->listNamespaces();
        const auto state = model->publicationState();
        if (state == CatalogManager::PublicationState::Current) return model->listNamespaces();
        if (state == CatalogManager::PublicationState::Conflict)
            throw DbError("58030", "catalog read conflicts with unpublished or missing metadata");
    }
    if (!ownership.mayReadCurrentFiles)
        throw DbError("55P03", "cold catalog read conflicts with an active DDL owner");
    // Cold and peer reads still load the complete catalog. Preserve validation
    // of unrelated catalog files and avoid bootstrap, migration or publication.
    return CatalogManager::readMetadataSnapshot(catalogDirForDb(engine_, dbname).string()).namespaces;
}

CatalogManager::DefaultCatalogSnapshot CatalogService::defaultCatalogSnapshot(
    const std::string& dbname, CatalogManager::DefaultCatalogKind kind, bool includeExpressions) const {
    const auto catalog = readCatalog(dbname);
    if (catalog.manager) return catalog.manager->defaultCatalogSnapshot(kind, includeExpressions);
    if (catalog.path.empty()) return {};
    return CatalogManager::readDefaultCatalogSnapshot(catalog.path, kind, includeExpressions);
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

namespace foreign_key_catalog {

void synchronize(const StorageEngine& engine, const std::string& database,
    CatalogManager& catalog, const PgClassRow& owner, const std::string& physical,
    const std::optional<std::pair<std::string, std::string>>& rename) {
    if (owner.relkind != 'r' || owner.relpersistence == 't' || owner.relispartition) return;
    const auto publicSpace = catalog.getNamespaceByName("public");
    auto table = engine.readPersistedSchemaForCatalog(database, physical);
    if (!table && publicSpace && owner.relnamespace == publicSpace->oid)
        table = engine.readPersistedSchemaForCatalog(database, "public__" + physical);
    if (!table || table->partitionType != TableSchema::PartitionType::None) return;
    CatalogManager::ForeignKeyReferences references;
    for (const auto& fk : table->fks) {
        const auto logical = CatalogService::logicalName(fk.refTable);
        const auto space = catalog.getNamespaceByName(logical.schema.empty() ? "public" : logical.schema);
        const auto qualified = space ? catalog.getClassByName(logical.name, space->oid) : std::nullopt;
        const auto plain = publicSpace ? catalog.getClassByName(fk.refTable, publicSpace->oid) : std::nullopt;
        if (qualified && plain && qualified->oid != plain->oid)
            throw DbError("XX001", "ambiguous foreign key target storage identity");
        const auto& target = qualified ? qualified : plain;
        if (fk.name.empty() || !target || target->relkind != 'r' || target->relispartition || target->relpersistence == 't') continue;
        if (!references.emplace(fk.name, target->oid).second)
            throw DbError("XX001", "duplicate stored foreign key name");
    }
    catalog.reconcileForeignKeyIdentities(owner.oid, references, rename);
}

ReferentialAction referentialAction(std::string_view stored) {
    if (stored == "noaction") return {'a', nullptr};
    if (stored == "restrict" || stored.empty()) return {'r', "RESTRICT"};
    if (stored == "cascade") return {'c', "CASCADE"};
    if (stored == "setnull") return {'n', "SET NULL"};
    throw DbError("XX001", "invalid stored foreign key action");
}

namespace {

struct StoredDefinition {
    const ForeignKey& key;
    Oid ownerNamespace;
    std::vector<int16_t> localColumns;
    std::vector<int16_t> referencedColumns;
    bool deferrable;
    bool initiallyDeferred;
    bool validated;
};

// Validate the owning table against the same catalog generation before
// projecting its fields or reconstructing SQL. The caller holds the read fence.
class DefinitionReader {
public:
    DefinitionReader(const StorageEngine& engine, const std::string& database)
        : engine_(engine), database_(database), metadata_(engine.catalogService().metadataSnapshot(database, false)) {
        for (const auto& row : metadata_.relations) relations_.emplace(row.oid, &row);
        for (const auto& space : metadata_.namespaces) namespaces_.emplace(space.oid, space.nspname);
    }

    const CatalogManager::MetadataSnapshot& metadata() const { return metadata_; }

    StoredDefinition read(const CatalogManager::ForeignKeyIdentities::value_type& entry) {
        checkForQueryInterrupt();
        const auto& [owner, name] = entry.first;
        const auto& child = relation(owner);
        const auto& parent = relation(entry.second.referenced);
        if (current_ != owner) {
            auto storage = physical(child);
            table_ = engine_.readPersistedSchemaForCatalog(database_, storage);
            if (!table_ && namespaces_.at(child.relnamespace) == "public") {
                storage = "public__" + storage;
                table_ = engine_.readPersistedSchemaForCatalog(database_, storage);
            }
            if (!table_) throw DbError("XX001", "foreign key catalog owner has no stored schema");
            params_ = engine_.getStorageParams(database_, storage);
            current_ = owner;
        }
        const auto fk = std::find_if(table_->fks.begin(), table_->fks.end(), [&](const auto& candidate) {
            return candidate.name == name;
        });
        if (fk == table_->fks.end()) throw DbError("XX001", "foreign key identity has no stored definition");
        const auto target = physical(parent);
        if ((fk->refTable != target && (namespaces_.at(parent.relnamespace) != "public" || fk->refTable != "public__" + target)) ||
            fk->colNames.empty() || fk->colNames.size() != fk->refCols.size())
            throw DbError("XX001", "foreign key identity differs from its stored definition");
        const auto flag = [&](const char* suffix, bool missing) {
            const auto setting = params_.find("constraint." + name + '.' + suffix);
            return setting == params_.end() ? missing : setting->second == "1";
        };
        return {*fk, child.relnamespace, columnNumbers(owner, fk->colNames),
            columnNumbers(parent.oid, fk->refCols), flag("deferrable", false),
            flag("initially_deferred", false), flag("validated", !flag("not_valid", false))};
    }

private:
    const StorageEngine& engine_;
    const std::string& database_;
    const CatalogManager::MetadataSnapshot metadata_;
    std::map<Oid, const PgClassRow*> relations_;
    std::map<Oid, std::string_view> namespaces_;
    Oid current_ = INVALID_OID;
    std::optional<TableSchema> table_;
    std::map<std::string, std::string> params_;

    const PgClassRow& relation(Oid oid) const {
        const auto found = relations_.find(oid);
        if (found == relations_.end()) throw DbError("XX001", "foreign key catalog relation is missing");
        return *found->second;
    }

    std::string physical(const PgClassRow& row) const {
        const auto space = namespaces_.find(row.relnamespace);
        if (space == namespaces_.end()) throw DbError("XX001", "foreign key catalog relation has no namespace");
        return space->second == "public" ? row.relname : std::string(space->second) + "__" + row.relname;
    }

    std::vector<int16_t> columnNumbers(Oid relation, const std::vector<std::string>& columns) const {
        std::vector<int16_t> numbers;
        numbers.reserve(columns.size());
        for (const auto& name : columns) {
            const auto field = std::find_if(metadata_.attributes.begin(), metadata_.attributes.end(), [&](const auto& attr) {
                return attr.attrelid == relation && attr.attname == name && attr.attnum > 0 && !attr.attisdropped;
            });
            if (field == metadata_.attributes.end()) throw DbError("XX001", "foreign key catalog field is missing");
            numbers.push_back(field->attnum);
        }
        return numbers;
    }
};

} // namespace

std::vector<std::vector<ExprValue>> rows(const StorageEngine& engine, const std::string& database,
    std::optional<Oid> ownerFilter, std::optional<Oid> referencedFilter) {
    const auto fence = engine.catalogReadOwnership(database);
    if (!fence.mayReadCurrentFiles)
        throw DbError("55P03", "foreign key catalog read conflicts with an active database writer");
    const auto keys = engine.catalogService().foreignKeyIdentities(database);
    if (keys.empty()) return {};
    DefinitionReader reader(engine, database);
    const auto keyType = DatumMetadataFactory(reader.metadata()).type(1005);
    if (!keyType || keyType->kind != DatumKind::Array || !keyType->arrayElement ||
        keyType->arrayElement->actualOid != 21)
        throw DbError("XX001", "foreign key column numbers have no smallint array type");
    const auto array = [&](const std::vector<int16_t>& columns) {
        std::string text = "{";
        auto value = std::make_shared<CompoundDatum>();
        value->type = keyType;
        value->dimensions.push_back({static_cast<int32_t>(columns.size()), 1});
        value->arrayElements.emplace();
        value->arrayElements->reserve(columns.size());
        value->arrayNullBitmap = false;
        for (const auto number : columns) {
            if (text.size() > 1) text += ',';
            const auto element = std::to_string(number);
            text += element;
            value->arrayElements->emplace_back("smallint", element);
        }
        ExprValue result("smallint[]", text + '}');
        result.compound = std::move(value);
        return result;
    };
    const auto oid = [](Oid value) { return ExprValue("oid", std::to_string(value)); };
    const auto boolean = [](bool value) { return ExprValue("boolean", value ? "t" : "f"); };
    const auto action = [](const std::string& value) {
        return ExprValue("\"char\"", std::string(1, referentialAction(value).code));
    };
    std::vector<std::vector<ExprValue>> result;
    const auto begin = ownerFilter ? keys.lower_bound({*ownerFilter, {}}) : keys.begin();
    for (auto entry = begin; entry != keys.end(); ++entry) {
        const auto& [owner, name] = entry->first;
        const auto& identity = entry->second;
        if (ownerFilter && owner != *ownerFilter) break;
        if (referencedFilter && identity.referenced != *referencedFilter) continue;
        const auto row = reader.read(*entry);
        result.push_back({oid(identity.oid), ExprValue("name", name), oid(row.ownerNamespace), ExprValue("\"char\"", "f"),
            boolean(row.deferrable), boolean(row.initiallyDeferred), boolean(true), boolean(row.validated),
            oid(owner), oid(0), oid(0), oid(identity.referenced), action(row.key.onUpdate), action(row.key.onDelete),
            boolean(true), ExprValue("smallint", "0"), boolean(false), array(row.localColumns), array(row.referencedColumns)});
    }
    return result;
}

std::optional<std::string> definition(const StorageEngine& engine, const std::string& database,
    Oid constraint, const Session* session) {
    if (!constraint) return {};
    const auto fence = engine.catalogReadOwnership(database);
    if (!fence.mayReadCurrentFiles)
        throw DbError("55P03", "foreign key definition read conflicts with an active database writer");
    const auto keys = engine.catalogService().foreignKeyIdentities(database, constraint);
    if (keys.empty()) return {};
    DefinitionReader reader(engine, database);
    const auto& entry = *keys.begin();
    const auto row = reader.read(entry);
    std::string sql = "FOREIGN KEY (";
    const auto appendColumns = [&](const std::vector<std::string>& columns) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i) sql += ", ";
            sql += SQLParser::quoteIdentifier(columns[i]);
        }
    };
    appendColumns(row.key.colNames);
    sql += ") REFERENCES " + regclass_detail::output(entry.second.referenced, reader.metadata(), session) + '(';
    appendColumns(row.key.refCols);
    sql += ')';
    if (const auto update = referentialAction(row.key.onUpdate).sql) sql += std::string(" ON UPDATE ") + update;
    if (const auto deletion = referentialAction(row.key.onDelete).sql) sql += std::string(" ON DELETE ") + deletion;
    if (row.deferrable) sql += " DEFERRABLE";
    if (row.initiallyDeferred) sql += " INITIALLY DEFERRED";
    if (!row.validated) sql += " NOT VALID";
    return sql;
}

} // namespace foreign_key_catalog

} // namespace dbms
