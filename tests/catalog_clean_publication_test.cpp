// Unchanged catalog publication must keep its already-durable physical image.
// Content, not setter-only dirty flags, determines whether an image changed.
#include "catalog/catalog.h"
#include <cassert>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

using namespace dbms;
namespace fs = std::filesystem;

#ifdef DBMS_CATALOG_FSYNC_FAULT_TEST
static bool failDirectorySync = false;
extern "C" int __real_fsync(int fd);
extern "C" int __wrap_fsync(int fd) {
    struct stat value{};
    if (failDirectorySync && ::fstat(fd, &value) == 0 && S_ISDIR(value.st_mode)) {
        failDirectorySync = false;
        errno = EIO;
        return -1;
    }
    return __real_fsync(fd);
}
#endif

static struct stat identity(const fs::path& path) {
    struct stat value{};
    assert(::stat(path.c_str(), &value) == 0);
    return value;
}

static bool unchanged(const struct stat& a, const struct stat& b) {
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino &&
        a.st_size == b.st_size && a.st_mtim.tv_sec == b.st_mtim.tv_sec &&
        a.st_mtim.tv_nsec == b.st_mtim.tv_nsec &&
        a.st_ctim.tv_sec == b.st_ctim.tv_sec &&
        a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
}

int main() {
    char directory[] = "/tmp/dbms-catalog-clean-XXXXXX";
    assert(::mkdtemp(directory));
    const fs::path root(directory);
    {
        CatalogManager owner(root.string());
        const Oid nsp = owner.createNamespace("public", 1);
        PgClassRow relation;
        relation.relname = "rows";
        relation.relnamespace = nsp;
        const Oid rel = owner.createClass(relation);
        owner.setDescription(rel, 1259, 0, "before");
        assert(owner.persistAll());
        const auto path = root / "pg_class.cat";
        const auto descriptionPath = root / "pg_description.cat";
        const auto first = identity(path);
        const auto firstDescription = identity(descriptionPath);
        for (int repetition = 0; repetition < 4; ++repetition)
            assert(owner.persistCurrent() == CatalogManager::PersistResult::Published);
        assert(unchanged(firstDescription, identity(descriptionPath)));
        if (!unchanged(first, identity(path))) {
            std::cerr << "unchanged catalog image was atomically republished\n";
            assert(false);
        }

        // A caller mutating an existing row cannot be hidden by a setter flag.
        auto* mutableRelation = const_cast<PgClassRow*>(owner.findClass(rel));
        assert(mutableRelation);
        mutableRelation->relpages = 73;
        assert(owner.persistAll());
        assert(!unchanged(first, identity(path)));
        auto snapshot = CatalogManager::readMetadataSnapshot(root.string());
        assert(snapshot.relations.size() == 1 && snapshot.relations[0].relpages == 73);
        const auto changed = identity(path);
        assert(owner.persistAll());
        assert(unchanged(changed, identity(path)));

        // A second clean cache adopts peer metadata instead of overwriting it.
        CatalogManager observer(root.string());
        owner.setDescription(rel, 1259, 0, "peer");
        assert(owner.persistAll());
        const auto description = identity(descriptionPath);
        assert(observer.persistAll());
        assert(observer.getDescription(rel, 1259, 0) == "peer");
        assert(unchanged(description, identity(descriptionPath)));

#ifdef DBMS_CATALOG_FSYNC_FAULT_TEST
        // Rename succeeded, but directory sync did not: retry cannot skip it.
        owner.setDescription(rel, 1259, 0, "failed-directory-sync");
        failDirectorySync = true;
        assert(!owner.persistAll());
        assert(!failDirectorySync);
        const auto failedGeneration = identity(descriptionPath);
        assert(owner.persistAll());
        assert(!unchanged(failedGeneration, identity(descriptionPath)));
        CatalogManager durableVerifier(root.string());
        assert(durableVerifier.getDescription(rel, 1259, 0) == "failed-directory-sync");
#endif
        // Conflicting local edits remain in memory, and must not erase a peer.
        observer.setDescription(rel, 1259, 0, "local-unsaved");
        owner.setDescription(rel, 1259, 0, "peer-again");
        assert(owner.persistAll());
        assert(!observer.persistAll());
        assert(observer.getDescription(rel, 1259, 0) == "local-unsaved");
        CatalogManager verifier(root.string());
        assert(verifier.getDescription(rel, 1259, 0) == "peer-again");
        observer.loadAll();
        assert(observer.listClasses().size() == 1);
        assert(observer.getDescription(rel, 1259, 0) == "peer-again");
        assert(observer.persistAll());

        // Physical rollback/replacement invalidates durable generation receipts.
        const auto saved = root / "class.saved";
        fs::copy_file(path, saved);
        mutableRelation = const_cast<PgClassRow*>(owner.findClass(rel));
        mutableRelation->relpages = 91;
        assert(owner.persistAll());
        fs::rename(saved, path);
        assert(owner.persistAll());
        assert(owner.findClass(rel)->relpages == 73);

        // Missing images are republished. A failed target cannot become clean.
        fs::remove(path);
        assert(owner.persistAll());
        assert(owner.findClass(rel)->relpages == 73);
        assert(fs::is_regular_file(path));
        fs::remove(path);
        fs::create_directory(path);
        assert(!owner.persistAll());
        assert(fs::is_directory(path));
        fs::remove(path);
        assert(owner.persistAll());
        snapshot = CatalogManager::readMetadataSnapshot(root.string());
        assert(snapshot.relations.size() == 1 && snapshot.relations[0].relpages == 73);

        // Same-size, same-mtime edits still invalidate a receipt (ctime differs).
        CatalogManager rawPeer(root.string());
        rawPeer.setDescription(rel, 1259, 0, "peer-again");
        assert(rawPeer.persistAll());
        const auto beforeExternal = identity(descriptionPath);
        std::ifstream descriptionInput(descriptionPath, std::ios::binary);
        std::string external((std::istreambuf_iterator<char>(descriptionInput)),
                              std::istreambuf_iterator<char>());
        const auto word = external.find("peer-again");
        assert(word != std::string::npos);
        external.replace(word, 10, "other-peer");
        std::ofstream output(descriptionPath, std::ios::binary | std::ios::trunc);
        output.write(external.data(), external.size());
        output.close();
        const timespec times[] = {beforeExternal.st_atim, beforeExternal.st_mtim};
        assert(::utimensat(AT_FDCWD, descriptionPath.c_str(), times, 0) == 0);
        assert(rawPeer.persistAll());
        assert(rawPeer.getDescription(rel, 1259, 0) == "other-peer");

        // Failed peer reload must neither throw from the bool persistence API
        // (also used by destructors) nor discard the previous in-memory model.
        assert(owner.persistAll());
        const auto priorDescription = owner.getDescription(rel, 1259, 0);
        const auto typePath = root / "pg_type.cat";
        const auto savedType = root / "type.saved";
        fs::copy_file(typePath, savedType);
        {
            std::ofstream invalid(typePath, std::ios::binary | std::ios::trunc);
            invalid << "not-a-catalog-record\n";
        }
        bool rejected = false;
        try { rejected = !owner.persistAll(); }
        catch (const std::exception& error) {
            std::cerr << "catalog publication threw during peer reload: " << error.what() << '\n';
        }
        assert(rejected);
        assert(owner.getDescription(rel, 1259, 0) == priorDescription);
        assert(owner.listClasses().size() == 1);
        fs::rename(savedType, typePath);
        assert(owner.persistAll());

        PgAttributeRow column;
        column.attrelid = rel;
        column.atttypid = 23;
        column.attnum = 1;
        column.attname = "old_column";
        owner.addAttribute(column);
        owner.setDescription(rel, PgClassOid_Class, 1, "dropped");
        owner.setDescription(rel, PgClassOid_Class, 2, "retained");
        assert(owner.persistAll());
        assert(!owner.hasUnpublishedChanges());
        const auto attributePath = root / "pg_attribute.cat";
        const auto savedAttributes = identity(attributePath);
        assert(owner.persistCurrent() == CatalogManager::PersistResult::Published);
        assert(unchanged(savedAttributes, identity(attributePath)));
        assert(owner.renameAttribute(rel, "old_column", "renamed_column"));
        assert(owner.hasUnpublishedChanges());
#ifdef DBMS_CATALOG_FSYNC_FAULT_TEST
        failDirectorySync = true;
        assert(owner.persistCurrent() == CatalogManager::PersistResult::Failed);
        assert(!failDirectorySync && owner.hasUnpublishedChanges());
        const auto failedAttributes = identity(attributePath);
#endif
        assert(owner.persistCurrent() == CatalogManager::PersistResult::Published);
#ifdef DBMS_CATALOG_FSYNC_FAULT_TEST
        assert(!unchanged(failedAttributes, identity(attributePath)));
#endif
        auto fields = CatalogManager::readMetadataSnapshot(root.string());
        assert(fields.attributes.size() == 1 && fields.attributes.front().attname == "renamed_column");

        // Saving once cannot make a previously lent attribute safe to reuse.
        auto* borrowed = const_cast<PgAttributeRow*>(owner.findAttribute(rel, "renamed_column"));
        assert(borrowed);
        for (int target : {91, 92}) {
            borrowed->attstattarget = target;
            assert(owner.hasUnpublishedChanges());
            assert(owner.persistCurrent() == CatalogManager::PersistResult::Published);
            fields = CatalogManager::readMetadataSnapshot(root.string());
            assert(fields.attributes.size() == 1 && fields.attributes.front().attstattarget == target);
        }
        assert(owner.remapColumnMetadataAfterDrop(rel, 1));
        assert(owner.hasUnpublishedChanges());
        assert(owner.persistDescriptions());
        assert(!owner.hasUnpublishedChanges());
        assert(CatalogManager::readDescription(root.string(), rel, 1, "pg_class") == "retained");
        assert(!CatalogManager::readDescription(root.string(), rel, 2, "pg_class"));

        // Repair still writes a missing file using the proven local image.
        fs::remove(descriptionPath);
        assert(owner.persistAll());
        assert(CatalogManager::readDescription(root.string(), rel, 1, "pg_class") == "retained");

        // A failed explicit load restores both unsaved rows and their state.
        owner.setDescription(rel, PgClassOid_Class, 1, "local description");
        fs::copy_file(typePath, savedType);
        {
            std::ofstream invalid(typePath, std::ios::binary | std::ios::trunc);
            invalid << "not-a-catalog-record\n";
        }
        rejected = false;
        try { owner.loadAll(); }
        catch (const std::exception&) { rejected = true; }
        assert(rejected);
        assert(owner.hasUnpublishedChanges());
        assert(owner.getDescription(rel, PgClassOid_Class, 1) == "local description");
        fs::rename(savedType, typePath);
        owner.loadAll();
        assert(!owner.hasUnpublishedChanges());
        assert(owner.getDescription(rel, PgClassOid_Class, 1) == "retained");
    }
    fs::remove_all(root);
    std::cout << "catalog clean publication, content edits and peer safety OK\n";
}
