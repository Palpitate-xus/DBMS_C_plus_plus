#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "commands/SequenceStorageName.h"
#include <filesystem>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>

namespace dbms {

namespace fs = std::filesystem;

CatalogService::CatalogService(const StorageEngine& engine) : engine_(engine) {}

CatalogService::~CatalogService() {
    (void)persistAll();
}

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
        const PgNamespaceRow* nameSpace =
            catalog.findNamespace(relation.relnamespace);
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
        const PgNamespaceRow* conflictingNamespace =
            catalog.findNamespaceByName(relation.relname.substr(0, dot));
        const PgClassRow* conflictingRelation = conflictingNamespace
            ? catalog.findClassByName(relation.relname.substr(dot + 1),
                                      conflictingNamespace->oid)
            : nullptr;
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

CatalogManager& CatalogService::get(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(dbname);
    if (it != cache_.end()) {
        return *it->second;
    }

    auto cat = std::make_unique<CatalogManager>(catalogDirForDb(engine_, dbname).string());
    cat->bootstrapSystemNamespaces();
    cat->bootstrapSystemTypes();
    migrateLegacyPublicDottedSequences(engine_, dbname, *cat);

    CatalogManager& ref = *cat;
    cache_.emplace(dbname, std::move(cat));
    return ref;
}

void CatalogService::evict(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(dbname);
}

bool CatalogService::persistAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = true;
    for (auto it = cache_.begin(); it != cache_.end();) {
        // Embedded callers and tests may remove a database directory outside
        // DROP DATABASE. Never let an orphaned catalog cache make an
        // unrelated checkpoint or transaction fail, and never recreate the
        // removed database from stale in-memory metadata.
        if (!engine_.databaseExists(it->first)) {
            it = cache_.erase(it);
            continue;
        }
        if (it->second && !it->second->persistAll()) ok = false;
        ++it;
    }
    return ok;
}

bool CatalogService::has(const std::string& dbname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.find(dbname) != cache_.end();
}

CatalogManager::QualifiedName CatalogService::logicalName(const std::string& physical) {
    CatalogManager::QualifiedName qn;
    qn.schema.clear();
    qn.name = physical;

    size_t pos = physical.find("__");
    if (pos != std::string::npos && pos > 0 && pos + 2 < physical.size()) {
        qn.schema = physical.substr(0, pos);
        qn.name = physical.substr(pos + 2);
    }
    return qn;
}

} // namespace dbms
