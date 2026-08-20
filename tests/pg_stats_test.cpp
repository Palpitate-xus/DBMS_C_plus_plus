// ============================================================================
// pg_stats_test — pg_stats/pg_statistic virtual view:
//   * ANALYZE persists per-column stats; getPgStatsRows() exposes one row
//     per (db, table, column) with the PG column shape
//   * schemaname/tablename/attname filters
//   * n_distinct follows PG conventions (-1 unique, negative fraction)
//   * queryPgCatalog routes pg_stats/pg_statistic with WHERE pushdown
// ============================================================================

#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using namespace dbms;

static void cleanupDb(const std::string& db) {
    if (g_engine.databaseExists(db)) {
        const auto status = g_engine.dropDatabase(db);
        assert(status == DBStatus::OK || status == DBStatus::DATABASE_NOT_FOUND);
    }
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
}

static std::vector<std::string> splitRow(const std::string& row) {
    std::vector<std::string> out;
    std::istringstream ss(row);
    std::string tok;
    while (ss >> tok) out.push_back(tok);
    return out;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("pgstats");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    TableSchema t;
    t.tablename = "stat_t";
    t.formatVersion = 2;
    t.append(makeIntColumn("id", false, 0, true));
    t.append(makeIntColumn("grp", false, 0, false));
    t.append(makeVarCharColumn("tag", false, 32, false));
    assert(g_engine.createTable(db, t) == DBStatus::OK);

    for (int i = 0; i < 20; ++i) {
        std::string grp = std::to_string(i % 4);   // 4 distinct values
        std::string tag = (i % 2 == 0) ? "even" : "odd";
        assert(g_engine.insert(db, "stat_t",
            {{"id", std::to_string(i)}, {"grp", grp}, {"tag", tag}})
            == DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "stat_t"));

    auto rows = g_engine.getPgStatsRows(db);
    assert(rows.size() == 3);  // id, grp, tag
    std::cout << "[PGSTATS] " << rows.size() << " columns exposed" << std::endl;

    // Column shape: schemaname tablename attname null_frac n_distinct
    // most_common_vals most_common_freqs histogram_bounds
    std::map<std::string, std::vector<std::string>> byCol;
    for (const auto& r : rows) {
        auto f = splitRow(r);
        assert(f.size() == 8);
        assert(f[0] == db);         // schemaname
        assert(f[1] == "stat_t");   // tablename
        byCol[f[2]] = f;
    }
    assert(byCol.count("id") && byCol.count("grp") && byCol.count("tag"));

    // id is unique across 20 rows → n_distinct = -1
    assert(byCol["id"][4] == "-1.000000");
    // grp: 4 distinct / 20 rows → -0.2
    double grpNd = atof(byCol["grp"][4].c_str());
    assert(grpNd > -0.21 && grpNd < -0.19);
    // tag: 2 distinct / 20 rows → -0.1
    double tagNd = atof(byCol["tag"][4].c_str());
    assert(tagNd > -0.11 && tagNd < -0.09);
    std::cout << "[PGSTATS] n_distinct conventions OK (id=-1, grp=" << grpNd
              << ", tag=" << tagNd << ")" << std::endl;

    // MCV: tag has {even,odd} each freq ~0.5
    assert(byCol["tag"][5].find("even") != std::string::npos);
    assert(byCol["tag"][5].find("odd") != std::string::npos);
    assert(atof(byCol["tag"][6].substr(1, 3).c_str()) > 0.4);
    std::cout << "[PGSTATS] most_common_vals/freqs OK" << std::endl;

    // Filters
    auto onlyGrp = g_engine.getPgStatsRows(db, "stat_t", "grp");
    assert(onlyGrp.size() == 1);
    assert(splitRow(onlyGrp[0])[2] == "grp");
    auto none = g_engine.getPgStatsRows(db, "stat_t", "nosuch");
    assert(none.empty());
    std::cout << "[PGSTATS] filters OK" << std::endl;

    // Catalog routing with WHERE pushdown
    auto routed = g_engine.queryPgCatalog(
        "pg_stats", {"=tablename stat_t", "=attname grp"}, {}, {});
    assert(routed.size() == 1);
    assert(splitRow(routed[0])[2] == "grp");
    auto routed2 = g_engine.queryPgCatalog(
        "pg_statistic", {"=tablename nosuch"}, {}, {});
    assert(routed2.empty());
    std::cout << "[PGSTATS] pg_catalog routing OK" << std::endl;

    cleanupDb(db);
    std::cout << "[PGSTATS] all pg_stats tests passed" << std::endl;
    return 0;
}
