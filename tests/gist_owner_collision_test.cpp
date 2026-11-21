#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "catalog/type_registry.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "common/Config.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
void need(bool value) {
    if (!value) throw std::runtime_error("GiST ownership check failed");
}

std::string readBytes(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    need(file.is_open());
    std::string bytes((std::istreambuf_iterator<char>(file)),
                      std::istreambuf_iterator<char>());
    need(!file.bad());
    return bytes;
}
}

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    g_config.autoAnalyzeEnabled = false;
    g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);

    const auto db = testDbPath("gist_owner_collision");
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    auto* oldSession = currentSession();
    setCurrentSession(&session);
    struct SessionGuard {
        Session* old;
        ~SessionGuard() { setCurrentSession(old); }
    } sessionGuard{oldSession};
    DdlExecutor ddl;
    unsigned failures = 0;

    need(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    for (const auto* sql : {
             "CREATE TABLE owner(id integer PRIMARY KEY,a_k text COLLATE \"C\")",
             "CREATE TABLE owner_a(id integer PRIMARY KEY,k text COLLATE \"C\")",
             "CREATE TABLE legacy_gist_source(id integer PRIMARY KEY,a_k text COLLATE \"C\")"}) {
        need(!ddl.executeSql(sql, session));
    }
    need(g_engine.insertRow(db, "owner_a", {{"id", std::string("1")}, {"k", std::string()}}) == DBStatus::OK);
    need(g_engine.insertRow(db, "owner_a", {{"id", std::string("2")}, {"k", std::string("x")}}) == DBStatus::OK);
    need(g_engine.insertRow(db, "owner", {{"id", std::string("1")}, {"a_k", std::string("z")}}) == DBStatus::OK);
    need(g_engine.insertRow(db, "owner", {{"id", std::string("2")}, {"a_k", std::nullopt}}) == DBStatus::OK);
    need(g_engine.insertRow(db, "legacy_gist_source", {{"id", std::string("1")}, {"a_k", std::string("z")}}) == DBStatus::OK);
    need(g_engine.insertRow(db, "legacy_gist_source", {{"id", std::string("2")}, {"a_k", std::nullopt}}) == DBStatus::OK);

    // Both indexes would use owner_a_k.gist. The first is valid; a second
    // CREATE must reject the collision before it can replace the first file.
    need(!ddl.executeSql("CREATE INDEX owner_a_idx ON owner_a USING gist(k)", session));
    const auto sharedPath = g_engine.dbPath(db) / "owner_a_k.gist";
    const auto firstBytes = readBytes(sharedPath);
    need(!firstBytes.empty());
    need(ddl.executeSql("CREATE INDEX owner_idx ON owner USING gist(a_k)", session));
    need(readBytes(sharedPath) == firstBytes);
    need(g_engine.createGiSTIndex(db, "owner", "a_k") == DBStatus::INVALID_VALUE);
    need(readBytes(sharedPath) == firstBytes);

    // Recreate the legacy ambiguous-file situation with a genuine GiST file
    // built from a different physical relation. Its values omit both sought
    // keys; blindly trusting it would lose matching rows from owner_a.
    need(!ddl.executeSql("CREATE INDEX legacy_gist_idx ON legacy_gist_source USING gist(a_k)", session));
    const auto donorPath = g_engine.dbPath(db) / "legacy_gist_source_a_k.gist";
    const auto donorBytes = readBytes(donorPath);
    const auto actualOwner = g_engine.getTableSchema(db, "owner_a");
    const auto aliasedOwner = g_engine.getTableSchema(db, "owner");
    const auto donor = g_engine.getTableSchema(db, "legacy_gist_source");
    need(actualOwner.physicalRelationId != aliasedOwner.physicalRelationId);
    need(actualOwner.physicalRelationId != donor.physicalRelationId);
    need(!donorBytes.empty() && donorBytes != firstBytes);
    need(std::filesystem::copy_file(donorPath, sharedPath,
                                    std::filesystem::copy_options::overwrite_existing));
    need(readBytes(sharedPath) == donorBytes);
    std::cout << "GIST_OWNER_SHARED_PATH " << sharedPath << " physical_ids="
              << actualOwner.physicalRelationId << ',' << aliasedOwner.physicalRelationId
              << ',' << donor.physicalRelationId << '\n';

    for (const auto& value : {std::string(), std::string("x")}) {
        const auto expected = value.empty() ? "1" : "2";
        auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(
            db, "SELECT id,k FROM owner_a WHERE k='" + value + "'"));
        auto plan = QueryPlanner::buildPreparedQueryPlan(&g_engine, db, query, query->ast.get());
        auto cursor = QueryPlanner::makePreparedCursor(std::move(plan), query->output);
        std::vector<ExprValue> cells;
        std::vector<std::string> ids;
        while (cursor->next(cells)) {
            need(cells.size() == 2 && !cells[0].isNull && !cells[1].isNull);
            ids.push_back(cells[0].value);
            need(cells[1].value == value);
        }
        cursor->close();
        const bool preparedOk = ids == std::vector<std::string>{expected};
        failures += !preparedOk;
        std::cout << "GIST_OWNER_PREPARED valuebytes=" << value.size()
                  << " expected=" << expected << " actual=";
        for (const auto& id : ids) std::cout << id << ',';
        std::cout << " pass=" << preparedOk << '\n';

        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        std::vector<int64_t> rids;
        (void)g_engine.query(db, "owner_a", {"=k '" + value + "'"}, {"id", "k"},
                             {}, false, false, false, 0, {}, &rows, &nulls, &rids);
        ids.clear();
        for (const auto& row : rows) {
            need(row.size() == 2);
            ids.push_back(row[0]);
        }
        const bool nativeOk = ids == std::vector<std::string>{expected};
        failures += !nativeOk;
        std::cout << "GIST_OWNER_NATIVE valuebytes=" << value.size()
                  << " expected=" << expected << " actual=";
        for (const auto& id : ids) std::cout << id << ',';
        std::cout << " pass=" << nativeOk << '\n';
    }
    std::cout << "GIST_OWNER_COLLISION failures=" << failures << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& error) {
    std::cerr << "GIST_OWNER_COLLISION_FATAL " << error.what() << '\n';
    return 2;
}
