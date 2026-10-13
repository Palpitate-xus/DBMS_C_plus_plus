#include "catalog/catalog.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace fs = std::filesystem;

static void independentRows(const fs::path& path) {
    std::optional<PgNamespaceRow> namespaceCopy;
    std::optional<PgClassRow> classCopy;
    std::optional<PgAttributeRow> attributeCopy;
    std::optional<PgTypeRow> typeCopy;
    std::optional<PgProcRow> procCopy;
    std::vector<PgProcRow> overloadCopies;
    Oid relation = INVALID_OID;
    {
        CatalogManager catalog(path.string());
        const Oid ns = catalog.createNamespace("public", 10);
        PgClassRow row;
        row.relname = "stable";
        row.relnamespace = ns;
        row.relpages = 17;
        relation = catalog.createClass(row);
        PgAttributeRow attribute;
        attribute.attrelid = relation;
        attribute.attname = "id";
        attribute.attnum = 1;
        attribute.atttypid = 23;
        catalog.addAttribute(attribute);
        PgTypeRow type;
        type.typname = "owned";
        type.typnamespace = ns;
        const Oid typeOid = catalog.createType(type);
        PgProcRow proc;
        proc.proname = "overloaded";
        proc.pronamespace = ns;
        proc.prosrc = "original";
        const Oid procOid = catalog.createProc(proc);
        proc.pronargs = 1;
        catalog.createProc(proc);

        namespaceCopy = catalog.getNamespace(ns);
        assert(namespaceCopy && catalog.getNamespaceByName("public")->oid == ns);
        classCopy = catalog.getClass(relation);
        attributeCopy = catalog.getAttribute(relation, "id");
        typeCopy = catalog.getType(typeOid);
        procCopy = catalog.getProc(procOid);
        overloadCopies = catalog.getProcsByName("overloaded", ns);
        assert(classCopy && attributeCopy && typeCopy && procCopy);
        assert(catalog.getClassByName("stable", ns)->oid == relation);
        assert(catalog.getTypeByName("owned", ns)->oid == typeOid);
        assert(catalog.resolveRelationSnapshot("public.stable", {})->oid == relation);
        assert(catalog.resolveRelationSnapshot("stable", {"public"})->oid == relation);
        assert(catalog.resolveAttributeSnapshot("stable", "id", {"public"})->attrelid == relation);
        assert(!catalog.getNamespace(999999) && !catalog.getNamespaceByName("missing"));
        assert(!catalog.getClass(999999) && !catalog.getClassByName("missing", ns));
        assert(!catalog.getAttribute(relation, "missing"));
        assert(!catalog.getType(999999) && !catalog.getTypeByName("missing", ns));
        assert(!catalog.getProc(999999) && catalog.getProcsByName("missing", ns).empty());
        assert(!catalog.resolveRelationSnapshot("missing", {"public"}));
        assert(!catalog.resolveAttributeSnapshot("stable", "missing", {"public"}));
        const Oid temporary = catalog.createTempNamespace(1234);
        assert(catalog.getTempNamespace(1234)->oid == temporary);
        assert(!catalog.getTempNamespace(5678));

        // Force normal vector growth for every row family after copies escape.
        for (int i = 0; i < 600; ++i) {
            catalog.createNamespace("ns" + std::to_string(i), 10);
            row.relname = "r" + std::to_string(i);
            catalog.createClass(row);
            attribute.attname = "a" + std::to_string(i);
            catalog.addAttribute(attribute);
            type.typname = "t" + std::to_string(i);
            catalog.createType(type);
            proc.proname = "p" + std::to_string(i);
            catalog.createProc(proc);
        }
        row = *classCopy;
        row.relpages = 99;
        assert(catalog.updateClass(relation, row));
        assert(catalog.renameAttribute(relation, "id", "renamed"));
        assert(catalog.persistAll());
        catalog.loadAll();
        assert(catalog.getClass(relation)->relpages == 99);
        assert(catalog.getAttribute(relation, "renamed"));
        assert(classCopy->relpages == 17 && attributeCopy->attname == "id");

        // Concurrent ordinary updates/readers: a value copy cannot combine
        // fields from different fully locked updateClass operations.
        std::atomic<bool> start{false};
        std::thread writer([&] {
            while (!start.load()) std::this_thread::yield();
            for (int i = 0; i < 2000; ++i) {
                PgClassRow replacement = *classCopy;
                replacement.relpages = i;
                replacement.relallvisible = i;
                assert(catalog.updateClass(relation, replacement));
            }
        });
        start.store(true);
        for (int i = 0; i < 2000; ++i) {
            const auto copy = catalog.getClass(relation);
            assert(copy && copy->oid == relation && copy->relname == "stable");
            assert((copy->relpages == 99 && copy->relallvisible == 0) ||
                   copy->relpages == copy->relallvisible);
            const auto column = catalog.resolveAttributeSnapshot("stable", "renamed", {"public"});
            assert(column && column->attrelid == relation && column->attname == "renamed");
        }
        writer.join();
        assert(classCopy->relpages == 17 && attributeCopy->attname == "id");
    }
    // Copies do not depend on the catalog object surviving.
    assert(namespaceCopy->nspname == "public");
    assert(classCopy->oid == relation && classCopy->relpages == 17);
    assert(attributeCopy->attname == "id" && attributeCopy->atttypid == 23);
    assert(typeCopy->typname == "owned");
    assert(procCopy->prosrc == "original");
    assert(overloadCopies.size() == 2 && overloadCopies[0].prosrc == "original" &&
           overloadCopies[1].pronargs == 1);
}

int main() {
    char pattern[] = "/tmp/dbms-catalog-value-copy-XXXXXX";
    char* created = ::mkdtemp(pattern);
    assert(created);
    const fs::path root(created);
    independentRows(root / "native");

    const std::string db = testDbPath("catalog_value_snapshot");
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    auto& catalog = g_engine.catalogService().get(db);
    const auto ns = catalog.getNamespaceByName("public");
    assert(ns);
    PgClassRow row;
    row.relname = "evicted_copy";
    row.relnamespace = ns->oid;
    row.relpages = 37;
    const Oid oid = catalog.createClass(row);
    PgAttributeRow attribute;
    attribute.attrelid = oid;
    attribute.attname = "evicted_attribute";
    attribute.attnum = 1;
    catalog.addAttribute(attribute);
    PgTypeRow type;
    type.typname = "evicted_type";
    type.typnamespace = ns->oid;
    const Oid typeOid = catalog.createType(type);
    PgProcRow proc;
    proc.proname = "evicted_proc";
    proc.pronamespace = ns->oid;
    proc.prosrc = "copy survives cache eviction";
    const Oid procOid = catalog.createProc(proc);
    assert(catalog.persistAll());
    const auto copy = catalog.getClass(oid);
    const auto attributeCopy = catalog.getAttribute(oid, "evicted_attribute");
    const auto typeCopy = catalog.getType(typeOid);
    const auto procCopy = catalog.getProc(procOid);
    assert(copy && copy->relpages == 37);
    g_engine.catalogService().evict(db);
    auto& reloaded = g_engine.catalogService().get(db);
    row = *reloaded.getClass(oid);
    row.relpages = 81;
    assert(reloaded.updateClass(oid, row) && reloaded.persistAll());
    assert(copy->relpages == 37 && copy->relname == "evicted_copy");
    g_engine.catalogService().evict(db);
    assert(copy->oid == oid && ns->nspname == "public");
    assert(attributeCopy && attributeCopy->attname == "evicted_attribute");
    assert(typeCopy && typeCopy->typname == "evicted_type");
    assert(procCopy && procCopy->prosrc == "copy survives cache eviction");
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    fs::remove_all(root);
    std::cout << "[CATALOG VALUE SNAPSHOT] growth/reload/destruction/evict/concurrent copies OK\n";
    return 0;
}
