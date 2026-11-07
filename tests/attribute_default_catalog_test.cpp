#include "catalog/catalog.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>

using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks; if (!value) throw std::runtime_error(reason);
}
std::map<std::string, std::string> bytes(const std::filesystem::path& root) {
    std::map<std::string, std::string> result;
    for (const auto& file : std::filesystem::recursive_directory_iterator(root)) if (file.is_regular_file()) {
        std::ifstream input(file.path(), std::ios::binary);
        result.emplace(file.path().lexically_relative(root).string(), std::string(std::istreambuf_iterator<char>(input), {}));
    }
    return result;
}
void relation(CatalogManager& catalog, Oid oid, const std::string& name) {
    PgClassRow table; table.oid = oid; table.relname = name; table.relnamespace = 2200; table.relnatts = 3;
    need(catalog.createClass(table) == oid, "create actual owner catalog identity");
    for (int16_t number = 1; number <= 3; ++number) {
        PgAttributeRow field; field.attrelid = oid; field.attnum = number;
        field.attname = std::string(1, static_cast<char>('a' + number - 1));
        field.atttypid = 2205; field.attlen = 4; field.attbyval = true;
        catalog.addAttribute(field);
    }
}
template<class Action> void corrupt(Action action, const std::string& reason) {
    bool rejected = false;
    try { action(); } catch (const DbError& error) { rejected = error.sqlState() == "XX001"; }
    need(rejected, reason);
}
}
int main() {
    try {
        const auto directory = testDbPath("attribute_default_catalog");
        cleanupTestDb("attribute_default_catalog");
        CatalogManager catalog(directory, false);
        catalog.bootstrapSystemNamespaces(); catalog.bootstrapSystemTypes();
        relation(catalog, 50000, "target"); relation(catalog, 50001, "owner");
        PgAttrdefRow definition; definition.adrelid = 50001; definition.adnum = 2;
        definition.expression = "CAST('50000' AS regclass)\n";
        const std::vector<std::pair<Oid, Oid>> targets{{PgClassOid_Class, 50000}, {PgClassOid_Class, 50000}, {PgClassOid_Type, 23}};
        const auto first = catalog.storeAttrdef(definition, targets);
        need(first && catalog.getAttrdef(first)->expression == definition.expression, "default has its own actual persistent identity");
        need(catalog.getAttribute(50001, "b")->atthasdef, "actual owner default flag published in model");
        const auto edges = catalog.findDepends(PgClassOid_Attrdef, first);
        need(edges.size() == 2 && std::count_if(edges.begin(), edges.end(), [](const auto& d) {
            return d.deptype == 'a' && d.refclassid == PgClassOid_Class && d.refobjid == 50001 && d.refobjsubid == 2;
        }) == 1, "one genuine column ownership edge, duplicate and pinned references omitted");
        need(std::count_if(edges.begin(), edges.end(), [](const auto& d) {
            return d.deptype == 'n' && d.refclassid == PgClassOid_Class && d.refobjid == 50000 && d.refobjsubid == 0;
        }) == 1, "one genuine target relation dependency");
        need(catalog.storeAttrdef(definition, targets, false) == first, "unchanged synchronization retains identity");
        const auto replaced = catalog.storeAttrdef(definition, targets, true);
        need(replaced != first && !catalog.getAttrdef(first) && catalog.getAttrdef(replaced), "explicit SET replaces default identity even for identical SQL");
        need(catalog.findDepends(PgClassOid_Attrdef, first).empty(), "old object's dependencies removed");
        need(catalog.persistAll(), "complete actual catalog persisted");
        const auto persisted = bytes(directory);
        const auto cold = CatalogManager::readMetadataSnapshot(directory);
        need(cold.defaults.size() == 1 && cold.defaults[0].oid == replaced &&
             cold.defaults[0].expression == definition.expression && cold.dependencies.size() >= 2,
             "independent cold snapshot owns default and dependency rows");
        need(bytes(directory) == persisted, "cold default reader has no filesystem effects");
        CatalogManager reloaded(directory, false);
        need(reloaded.getAttrdefForColumn(50001, 2)->oid == replaced &&
             reloaded.storeAttrdef(definition, targets, false) == replaced,
             "default identity and graph survive actual reload");
        const auto beforeFailure = reloaded.findAttrdefs(50001);
        corrupt([&] { reloaded.storeAttrdef(definition, {{PgClassOid_Class, 77777}}); }, "missing target rejected during complete preflight");
        auto wrong = definition; wrong.adnum = 9;
        corrupt([&] { reloaded.storeAttrdef(wrong, targets); }, "missing owner column rejected");
        need(reloaded.findAttrdefs(50001).size() == beforeFailure.size() &&
             reloaded.getAttrdef(replaced)->expression == definition.expression && bytes(directory) == persisted,
             "failed preflights do not expose partial catalog or disk edits");
        const auto restricted = reloaded.planDrop(PgClassOid_Class, 50000, CatalogManager::DropBehavior::Restrict);
        need(!restricted.ok() && reloaded.getClass(50000) && reloaded.getAttrdef(replaced), "normal default dependency blocks target RESTRICT without edits");
        const auto cascade = reloaded.planDrop(PgClassOid_Class, 50000, CatalogManager::DropBehavior::Cascade);
        need(cascade.ok() && cascade.objectsToDrop == std::vector<std::pair<Oid, Oid>>{{PgClassOid_Attrdef, replaced}, {PgClassOid_Class, 50000}},
             "CASCADE selects default and target while preserving the owner table and column");
        need(reloaded.applyDropPlan(cascade) && !reloaded.getClass(50000) && !reloaded.getAttrdef(replaced) &&
             reloaded.getClass(50001) && reloaded.getAttribute(50001, "b") && !reloaded.getAttribute(50001, "b")->atthasdef,
             "actual cascade removes only the default, target, and their dependencies");
        need(reloaded.persistAll(), "persist cascade");
        CatalogManager afterCascade(directory, false);
        need(afterCascade.findAttrdefs(50001).empty() && afterCascade.getClass(50001), "cascade survives actual reload");

        definition.adrelid = 50001; definition.adnum = 3; definition.expression = "CAST('50001' AS regclass)";
        const auto self = afterCascade.storeAttrdef(definition, {{PgClassOid_Class, 50001}});
        need(afterCascade.planDrop(PgClassOid_Class, 50001, CatalogManager::DropBehavior::Restrict).ok(),
             "self reference does not block dropping its own table");
        need(afterCascade.remapColumnMetadataAfterDrop(50001, 1), "compact a different physical column");
        auto compacted = afterCascade.findAttributes(50001);
        compacted.erase(compacted.begin());
        for (size_t i = 0; i < compacted.size(); ++i) compacted[i].attnum = static_cast<int16_t>(i + 1);
        need(afterCascade.replaceAttributes(50001, compacted), "publish the corresponding actual compacted field declaration");
        need(afterCascade.getAttrdef(self)->adnum == 2 && afterCascade.getAttrdefForColumn(50001, 2)->oid == self,
             "actual default follows its column number while retaining identity");
        const auto remappedEdges = afterCascade.findDepends(PgClassOid_Attrdef, self);
        need(std::any_of(remappedEdges.begin(), remappedEdges.end(),
                         [](const auto& d) { return d.deptype == 'a' && d.refobjsubid == 2; }),
             "column ownership edge follows compaction");
        need(afterCascade.dropAttrdef(self) && !afterCascade.getAttrdef(self) &&
             !afterCascade.getAttribute(50001, "c")->atthasdef, "explicit DROP DEFAULT removes its own object");

        // Corrupt a real persisted row and verify the failed refresh retains
        // the entire prior model. This is format validation, not an IO/OOM matrix.
        CatalogManager stable(directory, false);
        const auto original = bytes(directory);
        { std::ofstream output(std::filesystem::path(directory) / "pg_attrdef.cat", std::ios::trunc); output << "A1,1,50001,2,X00,X\n"; }
        corrupt([&] { stable.loadAll(); }, "invalid persisted expression rejected");
        need(stable.getClass(50001) && stable.findAttrdefs(50001).empty(), "failed refresh retains previous actual model");
        { std::ofstream output(std::filesystem::path(directory) / "pg_attrdef.cat", std::ios::trunc); output << original.at("pg_attrdef.cat"); }
        need(bytes(directory) == original, "corruption fixture restored exactly");

        relation(stable, 50002, "owned_sequence");
        PgDependRow ownership; ownership.classid = PgClassOid_Class; ownership.objid = 50002;
        ownership.refclassid = PgClassOid_Class; ownership.refobjid = 50001;
        ownership.refobjsubid = 1; ownership.deptype = 'a';
        stable.addDepend(ownership);
        definition.adrelid = 50001; definition.adnum = 1; definition.expression = "nextval('50002'::regclass)";
        const auto ownedDefault = stable.storeAttrdef(definition, {{PgClassOid_Class, 50002}});
        need(!stable.planDrop(PgClassOid_Class, 50002, CatalogManager::DropBehavior::Restrict).ok(),
             "standalone sequence RESTRICT is blocked by its normal default reference");
        const std::vector<std::pair<Oid, Oid>> roots{{PgClassOid_Class, 50002}, {PgClassOid_Attrdef, ownedDefault}};
        need(stable.planDrop(roots, CatalogManager::DropBehavior::Restrict).ok(),
             "removing the owning column can include its default and sequence as actual pending roots");
        definition.adnum = 2;
        const auto externalDefault = stable.storeAttrdef(definition, {{PgClassOid_Class, 50002}});
        need(!stable.planDrop(roots, CatalogManager::DropBehavior::Restrict).ok(),
             "another surviving column default still blocks the combined RESTRICT plan");
        const auto combined = stable.planDrop(roots, CatalogManager::DropBehavior::Cascade);
        need(combined.ok() && combined.objectsToDrop.size() == 3 &&
             std::find(combined.objectsToDrop.begin(), combined.objectsToDrop.end(),
                       std::make_pair(PgClassOid_Attrdef, externalDefault)) != combined.objectsToDrop.end(),
             "combined CASCADE includes the other default without deleting its owner");
        need(stable.applyDropPlan(combined) && stable.getClass(50001) &&
             !stable.getAttrdef(ownedDefault) && !stable.getAttrdef(externalDefault),
             "combined plan removes each genuine object once");
        std::cout << "ATTRIBUTE_DEFAULT_CATALOG_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ATTRIBUTE_DEFAULT_CATALOG_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}
