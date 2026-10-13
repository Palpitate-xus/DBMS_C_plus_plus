#include "catalog/catalog.h"
#include "common/DbError.h"
#include <filesystem>
#include <iostream>
#include <unistd.h>

using namespace dbms;
namespace fs = std::filesystem;

int main() {
    char pattern[] = "/tmp/dbms-catalog-optional-XXXXXX";
    char* created = ::mkdtemp(pattern);
    if (!created) return 2;
    const fs::path root(created);
    const fs::path absent = root / "not-bootstrapped" / "pg_catalog";
    int failures = 0;
    const auto require = [&](bool ok, const char* label) {
        if (!ok) { ++failures; std::cerr << "FAIL " << label << '\n'; }
    };
    try {
        const auto copy = CatalogManager::readMetadataSnapshot(absent.string());
        require(copy.namespaces.empty() && copy.relations.empty() &&
                copy.attributes.empty() && copy.types.empty(),
                "optional cold metadata is an empty snapshot");
    } catch (const DbError& error) {
        std::cerr << "cold snapshot: " << error.what() << '\n';
        require(false, "optional cold snapshot must not fail");
    }
    try {
        require(CatalogManager::readDomainColumns(absent.string(), "public", "rows").empty(),
                "optional cold domain metadata is empty");
    } catch (const DbError& error) {
        std::cerr << "cold domain metadata: " << error.what() << '\n';
        require(false, "optional cold domain metadata must not fail");
    }
    require(!fs::exists(absent) && !fs::exists(absent.parent_path()),
            "cold read does not bootstrap paths or files");

    const fs::path warmPath = root / "warm";
    const fs::path savedPath = root / "warm.saved";
    {
        CatalogManager warm(warmPath.string());
        const Oid ns = warm.createNamespace("public", 10);
        PgClassRow row;
        row.relname = "retained";
        row.relnamespace = ns;
        const Oid oid = warm.createClass(row);
        require(warm.persistAll(), "warm metadata persisted");
        fs::rename(warmPath, savedPath);
        bool rejected = false;
        try { warm.loadAll(); }
        catch (const DbError& error) { rejected = error.sqlState() == "58030"; }
        require(rejected, "disappearing loaded directory remains an I/O error");
        const auto rows = warm.listClasses();
        require(rows.size() == 1 && rows[0].oid == oid && rows[0].relname == "retained",
                "failed warm reload preserves old rows");
        require(!warm.persistAll() && !fs::exists(warmPath),
                "failed warm publication does not recreate an absent directory");
        fs::rename(savedPath, warmPath);
        warm.loadAll();
        require(warm.persistAll(), "healthy directory restoration permits retry");
    }
    fs::remove_all(root);
    std::cout << "catalog optional directory failures=" << failures << '\n';
    return failures == 0 ? 0 : 1;
}
