#include "catalog/catalog.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using dbms::CatalogManager;

int main() {
    char directory[] = "/tmp/dbms-publication-state-XXXXXX";
    assert(::mkdtemp(directory));
    const fs::path root(directory);
    {
        CatalogManager owner(root.string(), false);
        const auto ns = owner.createNamespace("public", 1);
        dbms::PgClassRow row;
        row.relname = "rows";
        row.relnamespace = ns;
        const auto relation = owner.createClass(row);
        owner.setDescription(relation, 1259, 0, "before");
        assert(owner.persistAll());
        const auto current = CatalogManager::PublicationState::Current;
        const auto peer = CatalogManager::PublicationState::CleanPeerChange;
        const auto conflict = CatalogManager::PublicationState::Conflict;
        assert(owner.publicationState() == current);

        CatalogManager observer(root.string(), false);
        owner.setDescription(relation, 1259, 0, "after!");
        assert(owner.persistAll());
        assert(observer.publicationState() == peer);
        assert(observer.getDescription(relation, 1259, 0) == "before");
        // Checking is pure: it neither adopts nor republishes a peer image.
        assert(observer.publicationState() == peer);
        observer.loadAll();
        assert(observer.publicationState() == current);
        assert(observer.getDescription(relation, 1259, 0) == "after!");

        auto* borrowed = const_cast<dbms::PgClassRow*>(observer.findClass(relation));
        assert(borrowed);
        borrowed->relpages = 91;
        assert(observer.publicationState() == current);
        owner.setDescription(relation, 1259, 0, "peer!!");
        assert(owner.persistAll());
        assert(observer.publicationState() == conflict);
        assert(observer.findClass(relation)->relpages == 91);
        assert(!observer.persistAll());
        observer.loadAll();

        const auto path = root / "pg_description.cat";
        struct stat prior{};
        assert(::stat(path.c_str(), &prior) == 0);
        std::ifstream input(path, std::ios::binary);
        std::string bytes((std::istreambuf_iterator<char>(input)), {});
        const auto word = bytes.find("peer!!");
        assert(word != std::string::npos);
        bytes.replace(word, 6, "other!");
        {
            std::ofstream output(path, std::ios::binary | std::ios::trunc);
            output << bytes;
        }
        const timespec times[] = {prior.st_atim, prior.st_mtim};
        assert(::utimensat(AT_FDCWD, path.c_str(), times, 0) == 0);
        assert(observer.publicationState() == peer);
        observer.loadAll();
        assert(observer.getDescription(relation, 1259, 0) == "other!");

        const auto saved = root / "description.saved";
        fs::rename(path, saved);
        assert(observer.publicationState() == conflict);
        fs::create_directory(path);
        assert(observer.publicationState() == conflict);
        fs::remove(path);
        fs::create_symlink(saved, path);
        assert(observer.publicationState() == conflict);
        fs::remove(path);
        fs::rename(saved, path);
        assert(observer.publicationState() == peer);
        observer.loadAll();
        assert(observer.publicationState() == current);
    }
    fs::remove_all(root);
    std::cout << "[CATALOG PUBLICATION STATE] passed\n";
}
