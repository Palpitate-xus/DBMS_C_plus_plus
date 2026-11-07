#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <spawn.h>
#include <sys/wait.h>

extern dbms::StorageEngine g_engine;
extern char** environ;
using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool condition, const std::string& reason) {
    ++checks;
    if (!condition) throw std::runtime_error(reason);
}
std::map<std::string, std::string> bytes(const std::filesystem::path& root) {
    std::map<std::string, std::string> result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        std::ifstream input(entry.path(), std::ios::binary);
        result.emplace(entry.path().lexically_relative(root).string(),
            std::string(std::istreambuf_iterator<char>(input), {}));
    }
    return result;
}
std::string state(const CatalogManager& catalog, Oid relation) {
    std::ostringstream output;
    for (const auto& a : catalog.findAttributesIncludingSystem(relation))
        output << a.attname << ':' << a.attnum << ':' << a.atttypid << ':'
               << a.attlen << ':' << a.attndims << ':' << a.atttypmod << ':'
               << a.attbyval << ':' << a.attalign << ':' << a.attstorage << ':'
               << a.attcollation << ':' << a.attstattarget << ':' << a.attnotnull << '\n';
    for (int column = 0; column <= 5; ++column)
        for (const auto& d : catalog.findDepends(PgClassOid_Class, relation, column))
            output << d.toString() << '\n';
    return output.str();
}
template<class Action> void rejected(Action action, const std::string& reason) {
    bool failure = false;
    try { action(); } catch (const DbError& error) { failure = error.sqlState() == "XX001"; }
    need(failure, reason);
}
}
int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 3 && std::string(argv[1]) == "--verify-existing") {
            const auto catalog = g_engine.catalogService().acquire(argv[2]);
            const auto relation = catalog->getClassByName("legacy_arrays", 2200)->oid;
            need(catalog->getAttribute(relation, "a")->atttypid == 1007 &&
                 catalog->getAttribute(relation, "b")->atttypid == 1016,
                 "existing database upgrades during global engine initialization");
            std::cout << "PHYSICAL_ARRAY_EXISTING_PROCESS_CHECKED=" << checks << " FAILED=0\n";
            return 0;
        }
        const auto database = testDbPath("physical_array_catalog_upgrade");
        cleanupTestDb("physical_array_catalog_upgrade");
        need(g_engine.createDatabase(database) == DBStatus::OK, "create real database");
        Session session; session.username = "admin"; session.permission = 1;
        session.currentDB = database;
        struct Scope {
            Session* previous = currentSession();
            Scope(Session& session) { setCurrentSession(&session); }
            ~Scope() { setCurrentSession(previous); }
        } scope(session);
        DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE legacy_arrays(id integer PRIMARY KEY,a integer[],b bigint[],c text[],v varchar(5)[])", session), "create physical array storage");
        need(!ddl.executeSql("CREATE SCHEMA array_owner", session) &&
             !ddl.executeSql("CREATE TABLE array_owner.rows(a integer[])", session), "non-public physical owner");
        need(!ddl.executeSql("CREATE TABLE retained_owner_rows(a integer[])", session), "physical base codec for legacy logical owner");
        auto catalog = g_engine.catalogService().acquire(database);
        const auto relation = catalog->getClassByName("legacy_arrays", 2200)->oid;
        const auto namedNamespace = catalog->getNamespaceByName("array_owner")->oid;
        const auto namedRelation = catalog->getClassByName("rows", namedNamespace)->oid;
        const auto retainedRelation = catalog->getClassByName("retained_owner_rows", 2200)->oid;
        Oid retainedType = 0;
        const auto directory = std::filesystem::path(database) / "pg_catalog";
        const auto marker = directory / ".physical_builtin_array_attributes_version";
        need(std::filesystem::is_regular_file(marker), "fresh generation is published");
        catalog.reset(); g_engine.catalogService().evict(database);
        const auto beforeRead = bytes(database);
        const auto physical = g_engine.readPersistedSchemaForCatalog(database, "legacy_arrays");
        need(physical && physical->len == 5 && physical->cols[1].isArray &&
             physical->cols[1].dataType == "int", "independent persisted array witness");
        need(!g_engine.readPersistedSchemaForCatalog(database, "absent") &&
             !g_engine.catalogService().has(database) && bytes(database) == beforeRead,
             "raw schema inspection neither enters catalog service nor writes");

        const std::vector<Oid> elements{23, 20, 25, 1043};
        {
            CatalogManager old(directory.string(), false);
            auto attributes = old.findAttributes(relation);
            for (size_t i = 1; i < attributes.size(); ++i) {
                auto& a = attributes[i]; a.atttypid = elements[i - 1];
                a.attbyval = false; a.attalign = 'i'; a.attstorage = 'p';
                a.attcollation = 0;
                for (const auto& d : old.findDepends(PgClassOid_Class, relation, a.attnum))
                    if (d.refclassid == PgClassOid_Type)
                        need(old.removeDepend(d.classid, d.objid, d.objsubid,
                            d.refclassid, d.refobjid, d.refobjsubid), "remove optional builtin edge");
            }
            attributes[3].attstorage = 'e'; attributes[3].attcollation = 950;
            attributes[4].attstattarget = 37;
            need(old.replaceAttributes(relation, attributes), "legacy element-OID image");
            auto named = old.findAttributes(namedRelation);
            named[0].atttypid = 23; named[0].attstorage = 'p';
            need(old.replaceAttributes(namedRelation, named), "legacy namespaced array image");
            // This older shape stores the logical domain identity solely in
            // pg_attribute. No expression or domain runtime is evaluated.
            PgTypeRow logical; logical.typname = "retained_array_owner";
            logical.typnamespace = 2200; logical.typtype = 'd';
            logical.typbasetype = 1007; logical.typcategory = 'A'; logical.typstorage = 'x';
            retainedType = old.createType(logical);
            auto retained = old.findAttributes(retainedRelation);
            retained[0].atttypid = retainedType;
            need(retainedType && old.replaceAttributes(retainedRelation, retained), "persist genuine logical type owner");
            PgDependRow edge; edge.classid = PgClassOid_Class; edge.objid = relation;
            edge.objsubid = 3; edge.refclassid = PgClassOid_Type; edge.refobjid = 20;
            old.addDepend(edge);
            need(old.persistAll(), "persist known legacy catalog");
        }
        need(std::filesystem::remove(marker), "mark fixture as unpublished old generation");
        const auto oldDisk = bytes(database);
        const auto cold = CatalogManager::readMetadataSnapshot(directory.string());
        const auto oldA = std::find_if(cold.attributes.begin(), cold.attributes.end(),
            [&](const auto& a) { return a.attrelid == relation && a.attname == "a"; });
        need(oldA != cold.attributes.end() && oldA->atttypid == 23 && oldA->attstorage == 'p' &&
             bytes(database) == oldDisk && !g_engine.catalogService().has(database),
             "cold reader exposes actual legacy bytes without normalization");
        pid_t child = 0;
        char childOption[] = "--verify-existing";
        char* childArguments[] = {argv[0], childOption, const_cast<char*>(database.c_str()), nullptr};
        need(::posix_spawn(&child, argv[0], nullptr, nullptr, childArguments, environ) == 0,
             "exec a fresh process over the actual closed old image");
        int childStatus = 0;
        need(::waitpid(child, &childStatus, 0) == child && WIFEXITED(childStatus) &&
             WEXITSTATUS(childStatus) == 0, "fresh process and global startup complete normally");
        catalog = g_engine.catalogService().acquire(database);
        const auto a = catalog->getAttribute(relation, "a");
        const auto b = catalog->getAttribute(relation, "b");
        const auto c = catalog->getAttribute(relation, "c");
        const auto v = catalog->getAttribute(relation, "v");
        need(a && a->atttypid == 1007 && a->attlen == -1 && a->attndims == 1 &&
             !a->attbyval && a->attalign == 'i' && a->attstorage == 'x', "actual integer array upgraded");
        need(b && b->atttypid == 1016 && b->attalign == 'd' && b->attstorage == 'x', "bigint array alignment upgraded");
        need(c && c->atttypid == 1009 && c->attstorage == 'e' && c->attcollation == 950,
             "explicit storage and collation preserved");
        need(v && v->atttypid == 1015 && v->atttypmod == 9 && v->attstattarget == 37 &&
             v->attcollation == 100, "typmod and statistics preserved, default collation upgraded");
        need(catalog->getAttribute(namedRelation, "a")->atttypid == 1007, "actual namespaced schema upgraded");
        need(catalog->getAttribute(retainedRelation, "a")->atttypid == retainedType,
             "catalog-owned user type is not reinterpreted as its physical base array");
        const auto edges = catalog->findDepends(PgClassOid_Class, relation, 3);
        need(edges.size() == 1 && edges[0].refobjid == 1016 && edges[0].deptype == 'n',
             "existing normal type dependency retargeted");
        need(catalog->findDepends(PgClassOid_Class, relation, 2).empty(),
             "pinned builtin dependency is not invented");
        const auto upgradedDisk = bytes(database);
        for (const auto& entry : oldDisk)
            if (entry.first.find(".stc") != std::string::npos || entry.first.find(".dt") != std::string::npos)
                need(upgradedDisk.at(entry.first) == entry.second, "physical schema and heap bytes preserved");
        const auto upgraded = state(*catalog, relation);
        catalog.reset(); g_engine.catalogService().evict(database);
        catalog = g_engine.catalogService().acquire(database);
        need(state(*catalog, relation) == upgraded && bytes(database) == upgradedDisk,
             "normal reopen is idempotent without publication");
        catalog.reset(); g_engine.catalogService().evict(database);

        const std::vector<CatalogManager::PhysicalArrayAttribute> witnesses{
            {relation, 2, "a", 23, true}, {relation, 3, "b", 20, true}};
        CatalogManager staged(directory.string(), false);
        auto attributes = staged.findAttributes(relation);
        attributes[1].atttypid = 23; attributes[1].attstorage = 'p';
        attributes[2].attname = "wrong_owner";
        need(staged.replaceAttributes(relation, attributes), "complete preflight conflict fixture");
        const auto conflict = state(staged, relation);
        rejected([&] { staged.bootstrapPhysicalArrayAttributes(witnesses); }, "later conflict rejects complete upgrade");
        need(state(staged, relation) == conflict && bytes(database) == upgradedDisk,
             "later conflict publishes no partial in-memory or disk changes");
        attributes[2].attname = "b";
        need(staged.replaceAttributes(relation, attributes), "restore owned second field");
        const auto legacy = state(staged, relation);
        rejected([&] { staged.bootstrapPhysicalArrayAttributes(witnesses, true); }, "published generation rejects old field");
        need(state(staged, relation) == legacy, "published rejection does not repair");
        need(staged.bootstrapPhysicalArrayAttributes(witnesses) &&
             !staged.bootstrapPhysicalArrayAttributes(witnesses, true), "unpublished upgrade then strict validation");
        PgDependRow unrelated; unrelated.classid = PgClassOid_Class; unrelated.objid = relation;
        unrelated.objsubid = 2; unrelated.refclassid = PgClassOid_Type; unrelated.refobjid = 17;
        staged.addDepend(unrelated);
        need(!staged.bootstrapPhysicalArrayAttributes(witnesses, true) &&
             staged.findDepends(PgClassOid_Class, relation, 2)[0].refobjid == 17,
             "unrelated type dependencies remain owned by their original producer");
        PgDependRow partial; partial.classid = PgClassOid_Class; partial.objid = relation;
        partial.objsubid = 2; partial.refclassid = PgClassOid_Type; partial.refobjid = 23;
        staged.addDepend(partial);
        rejected([&] { staged.bootstrapPhysicalArrayAttributes(witnesses, true); }, "published stale type edge rejected");
        need(staged.bootstrapPhysicalArrayAttributes(witnesses), "unpublished partially published dependency completes coherently");
        const auto completedEdges = staged.findDepends(PgClassOid_Class, relation, 2);
        need(completedEdges.size() == 2 &&
             std::any_of(completedEdges.begin(), completedEdges.end(), [](const auto& d) { return d.refobjid == 1007; }) &&
             std::any_of(completedEdges.begin(), completedEdges.end(), [](const auto& d) { return d.refobjid == 17; }),
             "completion changes the known type edge and preserves the unrelated edge");
        staged.addDepend(partial);
        const auto duplicate = state(staged, relation);
        rejected([&] { staged.bootstrapPhysicalArrayAttributes(witnesses); }, "ambiguous type dependencies rejected");
        need(state(staged, relation) == duplicate && bytes(database) == upgradedDisk,
             "ambiguous dependency failure has no partial edits");

        CatalogManager invalidPair(directory.string(), false);
        auto pair = *invalidPair.getType(1007); pair.typelem = 21;
        need(invalidPair.updateType(1007, pair), "conflicting actual array pair fixture");
        rejected([&] { invalidPair.bootstrapPhysicalArrayAttributes(witnesses); }, "actual pair identity is required");
        auto wrongWitness = witnesses; wrongWitness[0].elementType = 25;
        CatalogManager wrong(directory.string(), false);
        rejected([&] { wrong.bootstrapPhysicalArrayAttributes(wrongWitness); }, "physical element must match stored array");
        auto duplicateWitness = witnesses; duplicateWitness.push_back(witnesses[0]);
        rejected([&] { wrong.bootstrapPhysicalArrayAttributes(duplicateWitness); }, "duplicate ownership witness rejected");
        need(bytes(database) == upgradedDisk, "all rejected preflights leave persisted image exact");

        // A genuine mutable startup, rather than a direct helper call, must
        // reject a legacy field under the published generation marker.
        auto publishedFields = wrong.findAttributes(relation);
        publishedFields[1].atttypid = 23; publishedFields[1].attstorage = 'p';
        need(wrong.replaceAttributes(relation, publishedFields) && wrong.persistAll(), "persist published conflict");
        const auto publishedConflict = bytes(database);
        rejected([&] { (void)g_engine.catalogService().acquire(database); }, "mutable startup rejects published corruption");
        need(bytes(database) == publishedConflict && !g_engine.catalogService().has(database),
             "failed startup does not write or cache a repaired generation");
        std::cout << "PHYSICAL_ARRAY_CATALOG_UPGRADE_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "PHYSICAL_ARRAY_CATALOG_UPGRADE_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}
