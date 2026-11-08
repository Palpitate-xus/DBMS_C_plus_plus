#include "catalog/CatalogService.h"
#include "catalog/PgDefaultCatalog.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
using Kind = CatalogManager::DefaultCatalogKind;
using Projection = CatalogManager::DefaultCatalogSnapshot;
unsigned checks = 0;
void need(bool condition, const std::string& message) {
    ++checks;
    if (!condition) throw std::runtime_error(message);
}
template<class F> bool hasState(F fn, const std::string& state) {
    try { fn(); } catch (const DbError& e) { return e.sqlState() == state; }
    return false;
}
std::vector<std::vector<std::string>> cells(const Projection& snapshot, bool depend) {
    std::vector<std::vector<std::string>> result;
    for (const auto& row : default_catalog::rows(snapshot, depend)) {
        std::vector<std::string> values;
        for (const auto& field : row) values.push_back(field.typeName + ":" + field.value + ":" + (field.isNull ? "1" : "0"));
        result.push_back(std::move(values));
    }
    return result;
}
struct PeerResult {
    Projection sameAttrs, sameDepends, warmAttrs, warmDepends;
    std::string coldAttrs, coldDepends;
};
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        // Construct both other engines before the writer starts its transaction.
        StorageEngine warm, cold;
        warm.setBackgroundIntervals(60000, 60000); cold.setBackgroundIntervals(60000, 60000);
        const auto db = testDbPath("d28_projection_extra"); cleanupTestDb("d28_projection_extra");
        need(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "actual database creation");
        Session owner; owner.username = "admin"; owner.permission = 1; owner.currentDB = db;
        struct SessionScope { Session* old = currentSession(); explicit SessionScope(Session& value) { setCurrentSession(&value); }
            ~SessionScope() { setCurrentSession(old); } } scope(owner);
        const auto ddl = [&](const std::string& sql) { need(!DdlExecutor{}.executeSql(sql, owner), "DDL: " + sql); };
        ddl("CREATE TABLE proof_target(k integer)");
        ddl("CREATE TABLE proof_owner(r regclass DEFAULT 'proof_target', n integer DEFAULT 7)");
        (void)warm.catalogService().get(db); // one cached committed generation, unlike cold
        const auto fullBefore = g_engine.catalogService().metadataSnapshot(db);
        const auto attrsBefore = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef);
        const auto depsBefore = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Depend);
        const auto originalAttrs = cells(attrsBefore, false);
        const auto originalDeps = cells(depsBefore, true);
        need(fullBefore.defaultObjectsIncluded && fullBefore.defaults.size() == 2, "complete public snapshot retained");
        need(!originalAttrs.empty() && !originalDeps.empty(), "actual rows exist");
        need(hasState([&] { (void)g_engine.catalogService().defaultCatalogSnapshot(db, static_cast<Kind>(77)); }, "XX000"),
             "invalid kind rejected by owning manager");
        auto bad = depsBefore;
        auto relation = std::find_if(bad.declaration.relations.begin(), bad.declaration.relations.end(),
            [](const auto& r) { return r.oid == default_catalog::dependencyOid; });
        need(relation != bad.declaration.relations.end(), "depend relation declaration exists");
        relation->relkind = 'v';
        need(hasState([&] { (void)default_catalog::rows(bad, true); }, "XX001"), "corrupt depend relation rejected");
        bad = depsBefore;
        auto attribute = std::find_if(bad.declaration.attributes.begin(), bad.declaration.attributes.end(),
            [](const auto& a) { return a.attrelid == default_catalog::dependencyOid && a.attnum == 7; });
        need(attribute != bad.declaration.attributes.end(), "depend field declaration exists");
        attribute->atttypid = 23;
        need(hasState([&] { (void)default_catalog::rows(bad, true); }, "XX001"), "corrupt depend field rejected");
        bad = depsBefore; bad.declaration.types.clear();
        need(hasState([&] { (void)default_catalog::rows(bad, true); }, "XX001"), "missing depend row types rejected");
        need(g_engine.beginTransaction(db) == DBStatus::OK, "actual surrounding transaction");
        ddl("ALTER TABLE proof_owner ALTER COLUMN n SET DEFAULT 9");
        const auto writerAttrs = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef);
        const auto writerDeps = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Depend);
        need(cells(writerAttrs, false) != originalAttrs, "writer observes changed actual default identity");
        need(cells(writerDeps, true) != originalDeps, "writer observes changed actual dependencies");
        need(cells(attrsBefore, false) == originalAttrs && cells(depsBefore, true) == originalDeps,
             "earlier returned values remain unchanged after writer mutation");
        std::promise<PeerResult> promise; auto future = promise.get_future();
        std::thread peer([&] {
            try {
                Session backend; backend.username = "admin"; backend.permission = 1; backend.currentDB = db;
                SessionScope backendScope(backend);
                PeerResult r;
                r.sameAttrs = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef);
                r.sameDepends = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Depend);
                r.warmAttrs = warm.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef);
                r.warmDepends = warm.catalogService().defaultCatalogSnapshot(db, Kind::Depend);
                try { (void)cold.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef); }
                catch (const DbError& e) { r.coldAttrs = e.sqlState(); }
                try { (void)cold.catalogService().defaultCatalogSnapshot(db, Kind::Depend); }
                catch (const DbError& e) { r.coldDepends = e.sqlState(); }
                promise.set_value(std::move(r));
            } catch (...) { promise.set_exception(std::current_exception()); }
        });
        const auto ready = future.wait_for(std::chrono::seconds(5));
        if (ready != std::future_status::ready) {
            const auto rolledBack = g_engine.rollbackTransaction();
            peer.join();
            need(rolledBack == DBStatus::OK, "release after bounded peer wait");
            throw std::runtime_error("foreign native catalog projection blocked for at least five seconds");
        }
        PeerResult actual;
        try { actual = future.get(); } catch (...) { peer.join(); throw; }
        peer.join();
        need(cells(actual.sameAttrs, false) == originalAttrs && cells(actual.sameDepends, true) == originalDeps,
             "foreign thread on same engine sees committed projection");
        need(cells(actual.warmAttrs, false) == originalAttrs && cells(actual.warmDepends, true) == originalDeps,
             "foreign thread on warm separate engine sees committed projection");
        need(actual.coldAttrs == "55P03" && actual.coldDepends == "55P03",
             "cold separate engine fails closed while writer owns DDL");
        need(g_engine.rollbackTransaction() == DBStatus::OK, "actual rollback");
        need(cells(g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef), false) == originalAttrs &&
             cells(g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Depend), true) == originalDeps,
             "rollback restores both owner projections");
        need(cells(attrsBefore, false) == originalAttrs && cells(depsBefore, true) == originalDeps &&
             cells(writerAttrs, false) != originalAttrs && cells(writerDeps, true) != originalDeps,
             "both previously acquired generations stay immutable after rollback");
        const auto fullAfter = g_engine.catalogService().metadataSnapshot(db);
        need(fullAfter.defaultObjectsIncluded && fullAfter.defaults.size() == fullBefore.defaults.size() &&
             fullAfter.defaults[1].expression == fullBefore.defaults[1].expression,
             "complete public default expression snapshot remains intact after rollback");
        std::cout << "D28_PROJECTION_EXTRA_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "D28_PROJECTION_EXTRA_CHECKED=" << checks << " FAIL=" << e.what() << '\n';
        return 1;
    }
}
