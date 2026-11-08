#include "catalog/CatalogService.h"
#include "catalog/PgDefaultCatalog.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks;
    if (!value) throw std::runtime_error(reason);
}
std::map<std::string, std::string> bytes(const std::filesystem::path& root) {
    std::map<std::string, std::string> result;
    for (const auto& f : std::filesystem::recursive_directory_iterator(root)) if (f.is_regular_file()) {
        std::ifstream in(f.path(), std::ios::binary);
        result.emplace(f.path().lexically_relative(root).string(),
                       std::string(std::istreambuf_iterator<char>(in), {}));
    }
    return result;
}
bool equalRows(const std::vector<std::vector<ExprValue>>& a,
               const std::vector<std::vector<ExprValue>>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].size() != b[i].size()) return false;
        for (size_t j = 0; j < a[i].size(); ++j) {
            const auto& x = a[i][j]; const auto& y = b[i][j];
            if (x.typeName != y.typeName || x.value != y.value || x.isNull != y.isNull ||
                x.collation != y.collation || x.objectOid != y.objectOid || x.elementOids != y.elementOids ||
                x.recordFields || y.recordFields || x.compound || y.compound)
                return false;
        }
    }
    return true;
}
template<class F> bool corrupt(F&& fn) {
    try { fn(); } catch (const DbError& error) { return error.sqlState() == "XX001"; }
    return false;
}
}
int main() {
    try {
        using Kind = CatalogManager::DefaultCatalogKind;
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        const auto db = testDbPath("default_catalog_projection"); cleanupTestDb("default_catalog_projection");
        need(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "database");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = db;
        struct Scope { Session* previous = currentSession(); Scope(Session& s) { setCurrentSession(&s); }
            ~Scope() { setCurrentSession(previous); } } scope(session);
        const auto ddl = [&](const std::string& sql) { need(!DdlExecutor{}.executeSql(sql, session), "DDL: " + sql); };
        ddl("CREATE TABLE projection_target(k integer)");
        ddl("CREATE TABLE projection_owner(r regclass DEFAULT 'projection_target', v integer DEFAULT 7, t text DEFAULT '" +
            std::string(2048, 'x') + "')");
        const auto compare = [&](const CatalogManager::MetadataSnapshot& full,
                                 const CatalogManager::DefaultCatalogSnapshot& projected, bool dependencies) {
            need(projected.loaded, "projection actually loaded");
            need(dependencies ? projected.defaults.empty() : projected.dependencies.empty(), "other object collection omitted");
            need(equalRows(default_catalog::rows(full, dependencies), default_catalog::rows(projected, dependencies)),
                 "all result values, metadata and order match the complete snapshot");
        };
        const auto checkService = [&] {
            const auto full = g_engine.catalogService().metadataSnapshot(db);
            const auto attrs = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Attrdef);
            const auto deps = g_engine.catalogService().defaultCatalogSnapshot(db, Kind::Depend);
            need(full.defaultObjectsIncluded && full.defaults.size() == 3 && !full.dependencies.empty(),
                 "public snapshot still has all actual default expressions and dependencies");
            const auto owner = g_engine.catalogService().acquire(db);
            const auto relation = owner->getClassByName("projection_owner", 2200);
            need(bool(relation), "actual projection owner relation");
            const auto longDefault = std::find_if(full.defaults.begin(), full.defaults.end(),
                [&](const auto& d) { return d.adrelid == relation->oid && d.adnum == 3; });
            need(longDefault != full.defaults.end() &&
                 longDefault->expression.find(std::string(128, 'x')) != std::string::npos,
                 "long expression remains present in complete snapshot");
            compare(full, attrs, false); compare(full, deps, true);
            return attrs;
        };
        auto attrs = checkService();
        const auto rows = [&](const std::string& sql, const std::vector<std::vector<std::string>>& wanted) {
            auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db, sql));
            auto result = QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine, db, query, query->ast.get()));
            result.throwIfFailed(); need(result.structuredRows == wanted, "actual SQL rows: " + sql);
        };
        const auto sqlAttrs = [&] {
            rows("SELECT adnum FROM pg_attrdef WHERE adrelid='projection_owner'::regclass ORDER BY adnum", {{"1"}, {"2"}, {"3"}});
        };
        sqlAttrs();
        CatalogManager::DefaultCatalogSnapshot missing;
        need(corrupt([&] { (void)default_catalog::rows(missing, false); }), "unloaded snapshot rejected");
        need(corrupt([&] { (void)default_catalog::rows(attrs, true); }), "wrong catalog kind rejected");
        auto bad = attrs;
        auto found = std::find_if(bad.declaration.relations.begin(), bad.declaration.relations.end(),
            [](const auto& row) { return row.oid == default_catalog::relationOid; });
        need(found != bad.declaration.relations.end(), "actual attrdef declaration");
        const auto validRelation = *found;
        bad.declaration.relations.push_back(validRelation);
        need(corrupt([&] { (void)default_catalog::rows(bad, false); }), "duplicate OID and name rejected");
        bad = attrs;
        auto duplicate = validRelation;
        duplicate.oid += 1000000; bad.declaration.relations.push_back(duplicate);
        need(corrupt([&] { (void)default_catalog::rows(bad, false); }), "duplicate pg_catalog name rejected");
        bad = attrs;
        auto field = std::find_if(bad.declaration.attributes.begin(), bad.declaration.attributes.end(),
            [](const auto& a) { return a.attrelid == default_catalog::relationOid && a.attnum == 1; });
        need(field != bad.declaration.attributes.end(), "actual declaration field"); field->atttypid = 23;
        need(corrupt([&] { (void)default_catalog::rows(bad, false); }), "malformed declaration field rejected");
        const auto missingDb = g_engine.catalogService().defaultCatalogSnapshot(db + "_absent", Kind::Depend);
        need(corrupt([&] { (void)default_catalog::rows(missingDb, true); }), "missing database is not a fabricated empty catalog");
        need(g_engine.checkpoint(db), "checkpoint before cold reads");
        const auto directory = g_engine.dbPath(db) / "pg_catalog"; const auto before = bytes(directory);
        const auto coldFull = CatalogManager::readMetadataSnapshot(directory.string());
        compare(coldFull, CatalogManager::readDefaultCatalogSnapshot(directory.string(), Kind::Attrdef), false);
        compare(coldFull, CatalogManager::readDefaultCatalogSnapshot(directory.string(), Kind::Depend), true);
        need(bytes(directory) == before, "cold readers do not change catalog bytes");
        const auto expression = [&] {
            const auto owner = g_engine.catalogService().acquire(db);
            const auto rel = owner->getClassByName("projection_owner", 2200);
            need(bool(rel), "actual relation identity");
            const auto d = owner->getAttrdefForColumn(rel->oid, 2);
            need(bool(d), "actual second default identity");
            return d->expression;
        };
        const auto oldExpression = expression();
        need(g_engine.beginTransaction(db) == DBStatus::OK, "transaction");
        ddl("ALTER TABLE projection_owner ALTER COLUMN v SET DEFAULT 9");
        need(expression() != oldExpression, "transaction changed the real default expression");
        checkService(); sqlAttrs();
        need(g_engine.rollbackTransaction() == DBStatus::OK, "rollback");
        need(expression() == oldExpression, "rollback restored the real default expression");
        checkService(); sqlAttrs();
        std::cout << "DEFAULT_CATALOG_PROJECTION_CHECKED=" << checks << " FAILED=0\n"; return 0;
    } catch (const std::exception& error) {
        std::cerr << "DEFAULT_CATALOG_PROJECTION_CHECKED=" << checks << " FAIL=" << error.what() << '\n'; return 1;
    }
}
